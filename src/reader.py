import logging
import os
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession

load_dotenv()

logger = logging.getLogger(__name__)

# Local folder for the raw data imported from Azure
AZURE_RAW_DIR = "/home/jovyan/src/data/azure_tradecorp_raw"


class DataReader:

    @staticmethod
    def download_azure_raw(
        output_dir: str = AZURE_RAW_DIR,
        prefix: str = "",
        extensions: Optional[Iterable[str]] = None,
    ) -> List[str]:
        """Downloads the files of the Azure container into output_dir (keeps the folder structure).

        - prefix: only download blobs whose name starts with this prefix
        - extensions: only download these file types, e.g. (".csv",); None downloads everything
        Returns the list of local file paths.
        """
        # Imported here so the Silver job can import DataReader without the Azure SDK or credentials
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

        base_dir = Path(output_dir).resolve()
        allowed = tuple(ext.lower() for ext in extensions) if extensions else None

        downloaded: List[str] = []
        for blob in container_client.list_blobs(name_starts_with=prefix or None, include=["metadata"]):
            # With ADLS Gen2, directories are listed as zero-byte blobs: skip them
            metadata = blob.metadata or {}
            if blob.name.endswith("/") or metadata.get("hdi_isfolder") == "true":
                continue
            if allowed and not blob.name.lower().endswith(allowed):
                continue

            destination = (base_dir / blob.name).resolve()
            # A blob name such as "../../x.csv" must not write outside output_dir
            if base_dir not in destination.parents:
                raise ValueError(f"Invalid destination path for blob: {blob.name}")

            destination.parent.mkdir(parents=True, exist_ok=True)
            with open(destination, "wb") as file:
                # readinto streams to disk instead of loading the whole blob in memory
                container_client.download_blob(blob.name).readinto(file)

            logger.info("Downloaded %s to %s", blob.name, destination)
            downloaded.append(str(destination))

        return downloaded

    @staticmethod
    def read_parquet(spark: SparkSession, path: str) -> DataFrame:
        """Reads a single Parquet path into a Spark DataFrame."""
        return spark.read.parquet(path)

    @classmethod
    def load_bronze_data(
        cls,
        spark: SparkSession,
        bronze_path: str,
        table_names: Sequence[str],
    ) -> Dict[str, DataFrame]:
        """Loads multiple Bronze Parquet tables into a dictionary of DataFrames."""
        logger.info("Loading Bronze datasets from %s", bronze_path)
        return {
            name: cls.read_parquet(spark, f"{bronze_path}/{name}")
            for name in table_names
        }


if __name__ == "__main__":
    # `python reader.py` downloads the raw files from Azure; importing this module does nothing
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    files = DataReader.download_azure_raw()
    logger.info("%d file(s) downloaded to %s", len(files), AZURE_RAW_DIR)