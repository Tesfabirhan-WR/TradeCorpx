import logging
import os
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Union

from dotenv import load_dotenv
from pyspark.sql import DataFrame

load_dotenv()

logger = logging.getLogger(__name__)

# Local folder used as a staging area before Silver Parquet is uploaded to Azure
SILVER_LOCAL_DIR = "/home/jovyan/src/data/silver"


class DataWriter:

    # ------------------------------------------------------------------
    # Parquet (local)
    # ------------------------------------------------------------------

    @staticmethod
    def write_parquet_local(
        df: DataFrame,
        table_name: str,
        output_dir: str = SILVER_LOCAL_DIR,
        mode: str = "overwrite",
        partition_by: Optional[Union[str, Sequence[str]]] = None,
    ) -> str:
        """Writes a DataFrame to local Parquet at <output_dir>/<table_name>.

        Returns the local path written to.
        """
        output_path = f"{output_dir}/{table_name}"

        writer = df.write.mode(mode)
        if partition_by:
            columns = [partition_by] if isinstance(partition_by, str) else list(partition_by)
            writer = writer.partitionBy(*columns)

        writer.parquet(output_path)
        logger.info("Wrote %s to %s (mode=%s)", table_name, output_path, mode)
        return output_path

    # ------------------------------------------------------------------
    # Azure (upload local Parquet to Blob/ADLS Gen2)
    # ------------------------------------------------------------------

    @staticmethod
    def upload_azure_parquet(
        local_path: str,
        blob_prefix: str,
        overwrite: bool = True,
    ) -> List[str]:
        """Uploads every part file under local_path to the Azure container at blob_prefix.

        Mirrors DataReader.download_azure_raw but in the opposite direction:
        reads AZURE_STORAGE_ACCOUNT / AZURE_STORAGE_KEY / CONTAINER_NAME from
        the environment and walks local_path recursively, preserving the
        Parquet part-file names under blob_prefix/.
        Returns the list of blob names uploaded.
        """
        # Imported here so DataWriter can be imported without the Azure SDK or credentials
        from azure.storage.blob import BlobServiceClient

        account_name = os.getenv("AZURE_STORAGE_ACCOUNT")
        account_key = os.getenv("AZURE_STORAGE_KEY")
        container_name = os.getenv("CONTAINER_NAME")

        missing = [
            name
            for name, value in {
                "AZURE_STORAGE_ACCOUNT": account_name,
                "AZURE_STORAGE_KEY": account_key,
                "CONTAINER_NAME": container_name,
            }.items()
            if not value
        ]
        if missing:
            raise EnvironmentError(f"Missing environment variable(s): {', '.join(missing)}")

        connection_string = (
            f"DefaultEndpointsProtocol=https;"
            f"AccountName={account_name};"
            f"AccountKey={account_key};"
            f"EndpointSuffix=core.windows.net"
        )
        container_client = BlobServiceClient.from_connection_string(
            connection_string
        ).get_container_client(container_name)

        base_dir = Path(local_path).resolve()
        if not base_dir.exists():
            raise FileNotFoundError(f"local_path does not exist: {base_dir}")

        uploaded: List[str] = []
        for file_path in base_dir.rglob("*"):
            if file_path.is_dir():
                continue
            # Skip Spark's success/checksum markers, only ship the actual Parquet data
            if file_path.name.startswith("_") or file_path.name.startswith("."):
                continue

            relative_name = file_path.relative_to(base_dir)
            blob_name = f"{blob_prefix.rstrip('/')}/{relative_name.as_posix()}"

            with open(file_path, "rb") as file:
                container_client.upload_blob(blob_name, file, overwrite=overwrite)

            logger.info("Uploaded %s to %s", file_path, blob_name)
            uploaded.append(blob_name)

        return uploaded

    @classmethod
    def write_parquet_to_azure(
        cls,
        df: DataFrame,
        table_name: str,
        local_tmp_dir: str = SILVER_LOCAL_DIR,
        blob_prefix: str = "silver",
        mode: str = "overwrite",
        partition_by: Optional[Union[str, Sequence[str]]] = None,
    ) -> Dict[str, object]:
        """Writes df to local Parquet, then uploads it to Azure under blob_prefix/table_name.

        Returns {"local_path": ..., "blobs": [...]}.
        """
        local_path = cls.write_parquet_local(
            df, table_name, output_dir=local_tmp_dir, mode=mode, partition_by=partition_by
        )
        blobs = cls.upload_azure_parquet(local_path, blob_prefix=f"{blob_prefix}/{table_name}")
        return {"local_path": local_path, "blobs": blobs}

    # ------------------------------------------------------------------
    # PostgreSQL
    # ------------------------------------------------------------------

    @staticmethod
    def write_postgres(df: DataFrame, table_name: str, mode: str = "overwrite") -> str:
        """Writes a DataFrame to a PostgreSQL table via JDBC.

        Reads POSTGRES_JDBC_URL / POSTGRES_USER / POSTGRES_PASSWORD from the
        environment (POSTGRES_DRIVER optional, defaults to org.postgresql.Driver).
        """
        jdbc_url = os.getenv("POSTGRES_JDBC_URL")
        user = os.getenv("POSTGRES_USER")
        password = os.getenv("POSTGRES_PASSWORD")
        driver = os.getenv("POSTGRES_DRIVER", "org.postgresql.Driver")

        missing = [
            name
            for name, value in {
                "POSTGRES_JDBC_URL": jdbc_url,
                "POSTGRES_USER": user,
                "POSTGRES_PASSWORD": password,
            }.items()
            if not value
        ]
        if missing:
            raise EnvironmentError(f"Missing environment variable(s): {', '.join(missing)}")

        (
            df.write
            .mode(mode)
            .jdbc(
                url=jdbc_url,
                table=table_name,
                properties={"user": user, "password": password, "driver": driver},
            )
        )
        logger.info("Wrote %s to PostgreSQL (mode=%s)", table_name, mode)
        return table_name

    # ------------------------------------------------------------------
    # Combined Silver write
    # ------------------------------------------------------------------

    @classmethod
    def write_silver(
        cls,
        df: DataFrame,
        table_name: str,
        to_azure: bool = True,
        to_postgres: bool = True,
        local_tmp_dir: str = SILVER_LOCAL_DIR,
        blob_prefix: str = "silver",
        mode: str = "overwrite",
        partition_by: Optional[Union[str, Sequence[str]]] = None,
    ) -> Dict[str, object]:
        """Writes a cleaned Silver DataFrame to local Parquet, then optionally
        uploads it to Azure and/or writes it to PostgreSQL.

        Local Parquet is always written first since it's both the staging
        area for the Azure upload and a fast local copy for debugging.
        """
        results: Dict[str, object] = {
            "local_path": cls.write_parquet_local(
                df, table_name, output_dir=local_tmp_dir, mode=mode, partition_by=partition_by
            )
        }

        if to_azure:
            results["blobs"] = cls.upload_azure_parquet(
                results["local_path"], blob_prefix=f"{blob_prefix}/{table_name}"
            )

        if to_postgres:
            results["postgres_table"] = cls.write_postgres(df, table_name, mode=mode)

        return results


if __name__ == "__main__":
    # Example usage — wire this up from silver_pipeline.py in practice
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    # DataWriter.write_silver(df_orders_enriched, "orders_enriched")