from azure.storage.blob import BlobServiceClient
import os
from config.settings import AZURE_STORAGE
from utils.logger import logger

blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE["connection_string"])

def upload_to_blob(file_path, blob_name):
    """Uploads a local JSON file to Azure Blob Storage."""
    try:
        blob_client = blob_service_client.get_blob_client(container=AZURE_STORAGE["container_name"], blob=blob_name)
        with open(file_path, "rb") as file:
            blob_client.upload_blob(file, overwrite=True)
        logger.info(f"✅ Uploaded {blob_name} to Azure Blob Storage")
    except Exception as e:
        logger.error(f"❌ Azure Blob Upload Error: {e}")

def download_from_blob(blob_name, local_path):
    """Downloads a JSON file from Azure Blob Storage."""
    try:
        blob_client = blob_service_client.get_blob_client(container=AZURE_STORAGE["container_name"], blob=blob_name)
        with open(local_path, "wb") as file:
            file.write(blob_client.download_blob().readall())
        logger.info(f"📥 Downloaded {blob_name} from Azure Blob Storage")
    except Exception as e:
        logger.error(f"❌ Azure Blob Download Error: {e}")

def list_blobs():
    """Lists available blobs in Azure Storage."""
    try:
        container_client = blob_service_client.get_container_client(AZURE_STORAGE["container_name"])
        return [blob.name for blob in container_client.list_blobs()]
    except Exception as e:
        logger.error(f"❌ Azure Blob List Error: {e}")
        return []
