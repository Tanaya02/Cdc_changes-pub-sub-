from azure.storage.blob import BlobServiceClient
from config.azure_storage import AZURE_STORAGE_CONFIG
import json

def list_blobs():
    try:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONFIG["connection_string"])
        container_client = blob_service_client.get_container_client(AZURE_STORAGE_CONFIG["container_name"])

        blobs = [blob.name for blob in container_client.list_blobs()]
        print("Available blobs:", blobs)

        if "cdc_changes.json" not in blobs:
            print("❌ 'cdc_changes.json' does not exist!")
        else:
            print("✅ 'cdc_changes.json' is available.")

    except Exception as e:
        print(f"Error listing blobs: {str(e)}")

list_blobs()

def upload_to_blob(data):
    try:
        file_path = "cdc_changes.json"
        with open(file_path, "w") as f:
            json.dump(data, f)

        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONFIG["connection_string"])
        blob_client = blob_service_client.get_blob_client(container=AZURE_STORAGE_CONFIG["container_name"], blob=file_path)

        with open(file_path, "rb") as f:
            blob_client.upload_blob(f, overwrite=True)

        print("✅ CDC Changes Uploaded to Azure Blob")
        
    except Exception as e:
        print(f"Azure Blob upload error: {str(e)}")

upload_to_blob([{"id": 1, "name": "Test"}])
