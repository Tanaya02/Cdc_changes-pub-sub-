import pyodbc
import json
import os
from azure.storage.blob import BlobServiceClient
from config.azure_storage import AZURE_STORAGE_CONFIG
from utils.queue_handler import publish_to_queue
from utils.logger import log_info, log_error
import datetime
from config.db_config import DB_CONFIG

def get_cdc_enabled_tables():
    """Fetch all CDC-enabled tables dynamically from SQL Server."""
    try:
        conn = pyodbc.connect(
            f"DRIVER={{SQL Server}};SERVER={DB_CONFIG['server']};DATABASE={DB_CONFIG['database']};UID={DB_CONFIG['username']};PWD={DB_CONFIG['password']}"
        )
        cursor = conn.cursor()
        
        query = """
        SELECT s.name + '.' + t.name AS table_name
        FROM sys.tables t
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE t.is_tracked_by_cdc = 1
        """
        
        cursor.execute(query)
        tables = [row[0] for row in cursor.fetchall()]
        
        log_info(f"Detected CDC-enabled tables: {tables}")
        return tables

    except Exception as e:
        log_error(f"Error fetching CDC tables: {str(e)}")
        return []
    finally:
        cursor.close()
        conn.close()

def get_last_processed_lsn(table_name):
    """Retrieve the last processed LSN for each table."""
    file_name = f"last_lsn_{table_name.replace('.', '_')}.txt"
    try:
        with open(file_name, "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        return None  # First-time execution

def save_last_processed_lsn(table_name, lsn):
    """Save the latest processed LSN to prevent duplicate processing."""
    file_name = f"last_lsn_{table_name.replace('.', '_')}.txt"
    with open(file_name, "w") as f:
        f.write(lsn)

def extract_cdc_changes():
    """Extract CDC changes from all CDC-enabled tables."""
    all_changes = []
    
    try:
        conn = pyodbc.connect(
            f"DRIVER={{SQL Server}};SERVER={DB_CONFIG['server']};DATABASE={DB_CONFIG['database']};UID={DB_CONFIG['username']};PWD={DB_CONFIG['password']}"
        )
        cursor = conn.cursor()
        
        cdc_tables = get_cdc_enabled_tables()
        
        for table in cdc_tables:
            cdc_table_name = f"cdc.{table.replace('.', '_')}_CT"
            last_lsn = get_last_processed_lsn(table)

            query = f"SELECT * FROM {cdc_table_name}"
            if last_lsn:
                query += f" WHERE __$start_lsn > CONVERT(VARBINARY, '{last_lsn}', 1)"

            log_info(f"Executing CDC query for {table}...")
            cursor.execute(query)
            
            columns = [column[0] for column in cursor.description]
            changes = cursor.fetchall()

            if not changes:
                log_info(f"No new CDC changes found for {table}.")
                continue

            data = [
                {
                    "_source_table": table,
                    **{
                        columns[i]: (f"0x{row[i].hex().upper()}" if isinstance(row[i], bytes) else row[i])
                        for i in range(len(columns))
                    }
                }
                for row in changes
            ]

            if data:
                log_info(f"Sample CDC record for {table}: {json.dumps(data[0], default=str, indent=2)}")
                save_last_processed_lsn(table, data[-1]["__$start_lsn"])  # Save last LSN
            
            all_changes.extend(data)

    except Exception as e:
        log_error(f"CDC extraction error: {str(e)}")
        return None
    finally:
        cursor.close()
        conn.close()

    return all_changes

def serialize_data(data):
    """Serialize CDC data for storage and queue processing."""
    try:
        if isinstance(data, list):
            serialized = [
                {
                    "_source_table": row["_source_table"],  # Correctly reference '_source_table'
                    **{
                        k: (v.isoformat() if isinstance(v, datetime.datetime) else v)
                        for k, v in row.items() if k != "_source_table"  # Exclude '_source_table' from nested serialization
                    }
                }
                for row in data
            ]
            log_info(f"Successfully serialized {len(serialized)} records")
            return serialized
    except Exception as e:
        log_error(f"Serialization error: {str(e)}")
    
    return data

def upload_to_blob(data):
    """Upload CDC changes to Azure Blob Storage."""
    try:
        file_path = "cdc_changes.json"
        serialized_data = serialize_data(data)

        log_info(f"Writing {len(serialized_data)} records to {file_path}")

        with open(file_path, "w") as f:
            json.dump(serialized_data, f, indent=2)
            log_info(f"JSON file created successfully: {os.path.getsize(file_path)} bytes")

        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONFIG["connection_string"])
        blob_client = blob_service_client.get_blob_client(container=AZURE_STORAGE_CONFIG["container_name"], blob=file_path)

        with open(file_path, "rb") as f:
            blob_client.upload_blob(f, overwrite=True)

        log_info("✅ CDC Changes uploaded to Azure Blob successfully")

        downloaded_content = blob_client.download_blob().readall()
        log_info(f"Verified blob size: {len(downloaded_content)} bytes")

        os.remove(file_path)
        log_info(f"Cleaned up local file: {file_path}")

    except Exception as e:
        log_error(f"Azure Blob upload error: {str(e)}")

def main():
    """Main execution function."""
    try:
        log_info("Starting CDC extraction...")
        cdc_changes = extract_cdc_changes()

        if cdc_changes:
            log_info(f"Processing {len(cdc_changes)} CDC changes")
            publish_to_queue(cdc_changes)  # Send CDC changes to queue
            upload_to_blob(cdc_changes)  # Upload CDC changes to Blob Storage
            log_info("CDC processing completed successfully")
        else:
            log_info("No CDC changes to process")

    except Exception as e:
        log_error(f"Error in main process: {str(e)}")

if __name__ == "__main__":
    main()
