import json
import snowflake.connector
from azure.storage.blob import BlobServiceClient
from config.azure_storage import AZURE_STORAGE_CONFIG
from config.db_config import SNOWFLAKE_CONFIG
from utils.logger import log_info, log_error

CDC_FILE = "cdc_changes.json"
# "__$start_lsn""__$operation""__$command_id"
EXCLUDED_COLUMNS = {
     "__$end_lsn", "__$seqval", 
    "__$update_mask"
}

def connect_snowflake():
    """Establish connection to Snowflake."""
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_CONFIG["user"],
            password=SNOWFLAKE_CONFIG["password"],
            account=SNOWFLAKE_CONFIG["account"],
            database=SNOWFLAKE_CONFIG["database"],
            schema=SNOWFLAKE_CONFIG["schema"]
        )
        log_info("Successfully connected to Snowflake")
        return conn
    except Exception as e:
        log_error(f"❌ Snowflake Connection Failed: {e}")
        return None

def table_exists(table_name):
    """Check if the CDC table exists in Snowflake."""
    conn = connect_snowflake()
    if not conn:
        return False

    cursor = conn.cursor()
    try:
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        exists = cursor.fetchone() is not None
        return exists
    finally:
        cursor.close()
        conn.close()

def create_table_if_not_exists(json_data):
    """Creates table dynamically based on JSON structure, excluding CDC metadata columns."""
    table_name = json_data[0].get("_source_table")
    if not table_name:
        log_error("❌ '_source_table' key is missing in JSON data")
        return

    table_name = table_name.replace(".", "_")  # Convert to Snowflake-compatible format

    if table_exists(table_name):
        return

    columns = []
    for key, value in json_data[0].items():
        if key in EXCLUDED_COLUMNS or key == "_source_table":  # Exclude metadata and table name
            continue
        if isinstance(value, int):
            columns.append(f'"{key}" INT')
        elif isinstance(value, float):
            columns.append(f'"{key}" FLOAT')
        elif isinstance(value, str):
            columns.append(f'"{key}" STRING')
        else:
            columns.append(f'"{key}" VARIANT')

    column_definitions = ", ".join(columns)
    create_table_query = f"""
    CREATE TABLE {SNOWFLAKE_CONFIG['database']}.{SNOWFLAKE_CONFIG['schema']}.{table_name} 
    ({column_definitions})
    """
    
    conn = connect_snowflake()
    cursor = conn.cursor()
    try:
        cursor.execute(create_table_query)
        log_info(f"✅ Table '{table_name}' created successfully")
    finally:
        cursor.close()
        conn.close()

def get_existing_columns(table_name):
    """Fetch existing column names from Snowflake."""
    conn = connect_snowflake()
    cursor = conn.cursor()
    
    query = f"""
    SELECT COLUMN_NAME 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_CATALOG = '{SNOWFLAKE_CONFIG['database']}'
    AND TABLE_SCHEMA = '{SNOWFLAKE_CONFIG['schema']}' 
    AND TABLE_NAME = '{table_name.upper()}'
    """  # Ensure table name is in uppercase as Snowflake stores names in uppercase by default
    
    cursor.execute(query)
    columns = {row[0].lower() for row in cursor.fetchall()}  # Convert to lowercase for case-insensitive comparison
    
    cursor.close()
    conn.close()
    return columns

def add_missing_columns(record):
    """Add any missing columns based on the JSON data, excluding CDC metadata columns."""
    table_name = record.get("_source_table")
    if not table_name:
        log_error("❌ '_source_table' key is missing in record")
        return

    table_name = table_name.replace(".", "_")  # Convert to Snowflake-compatible format

    # Fetch existing columns (case-insensitive)
    existing_columns = get_existing_columns(table_name)
    conn = connect_snowflake()
    cursor = conn.cursor()

    for column, value in record.items():
        if column.lower() in existing_columns or column in EXCLUDED_COLUMNS or column == "_source_table":
            log_info(f"⚠️ Column '{column}' already exists or is excluded. Skipping addition.")
            continue
        if isinstance(value, int):
            col_type = "INT"
        elif isinstance(value, float):
            col_type = "FLOAT"
        elif isinstance(value, str):
            col_type = "STRING"
        else:
            col_type = "VARIANT"

        alter_query = f'ALTER TABLE {SNOWFLAKE_CONFIG["schema"]}.{table_name} ADD COLUMN "{column}" {col_type}'
        try:
            cursor.execute(alter_query)
            log_info(f"✅ Added new column: {column}")
            existing_columns.add(column.lower())  # Add the new column to the set to avoid duplicate attempts
        except Exception as e:
            log_error(f"❌ Error adding column '{column}': {e}")

    cursor.close()
    conn.close()

def record_exists_in_snowflake(record, cursor):
    """
    Check if a record already exists in Snowflake before inserting.
    Handles case-insensitive column name matching.
    """
    # Identify unique columns for deduplication (e.g., primary key or unique identifier)
    unique_columns = [col for col in record.keys() if col.lower() not in EXCLUDED_COLUMNS and col != "_source_table"]
    
    if not unique_columns:
        log_error("⚠️ No unique columns found in record for deduplication. Skipping record.")
        return False

    # Safely quote the table and column names
    table_name = record.get("_source_table")
    if not table_name:
        log_error("❌ '_source_table' key is missing in record")
        return False

    table_name = f"{SNOWFLAKE_CONFIG['database']}.{SNOWFLAKE_CONFIG['schema']}.{table_name.replace('.', '_')}"

    # Build the WHERE clause for deduplication
    where_clause = " AND ".join([f'"{col}" = %s' for col in unique_columns])
    values = tuple(record[col] for col in unique_columns)

    try:
        query = f"SELECT COUNT(*) FROM {table_name} WHERE {where_clause}"
        cursor.execute(query, values)
        
        exists = cursor.fetchone()[0] > 0
        return exists
    except Exception as e:
        log_error(f"❌ Error checking record existence: {e}")
        return False

def download_and_process_blob():
    """Download CDC JSON from Azure and process it."""
    try:
        # Download from Azure Blob
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONFIG["connection_string"])
        blob_client = blob_service_client.get_blob_client(
            container=AZURE_STORAGE_CONFIG["container_name"], 
            blob=CDC_FILE
        )
        
        download_stream = blob_client.download_blob()
        json_data = json.loads(download_stream.readall())
        log_info(f"📥 Downloaded CDC JSON from Azure Blob Storage: {len(json_data)} records")

        if not json_data:
            log_info("⚠️ No records to process")
            return

        # Group records by '_source_table'
        records_by_table = {}
        for record in json_data:
            table_name = record.get("_source_table")
            if not table_name:
                log_error("❌ '_source_table' key is missing in record")
                continue
            table_name = table_name.replace(".", "_")
            records_by_table.setdefault(table_name, []).append(record)

        # Process each table's records
        for table_name, records in records_by_table.items():
            log_info(f"Processing table: {table_name} with {len(records)} records")

            # Ensure table exists with proper schema
            create_table_if_not_exists(records)
            add_missing_columns(records[0])

            # Insert data
            conn = connect_snowflake()
            cursor = conn.cursor()

            try:
                # Filter out existing records
                filtered_data = []
                unique_records = set()
                
                for record in records:
                    # Remove CDC metadata columns
                    filtered_record = {k: v for k, v in record.items() if k not in EXCLUDED_COLUMNS and k != "_source_table"}

                    # Generate a unique key for deduplication (e.g., based on sorted key-value pairs)
                    record_key = tuple(sorted(filtered_record.items()))
                    
                    # Check for duplicates within the batch
                    if record_key in unique_records:
                        log_info(f"⚠️ Duplicate record found in batch for table: {table_name}. Skipping.")
                        continue
                    
                    # Check for duplicates in Snowflake
                    if not record_exists_in_snowflake(record, cursor):
                        filtered_data.append(filtered_record)
                        unique_records.add(record_key)
                    else:
                        log_info(f"⚠️ Record already exists in Snowflake for table: {table_name}. Skipping.")

                if not filtered_data:
                    log_info(f"⚠️ No new records to insert for table: {table_name}")
                    continue

                # Log details about filtered records
                log_info(f"Total records for table {table_name}: {len(records)}")
                log_info(f"Unique records to insert for table {table_name}: {len(filtered_data)}")

                column_list = ", ".join([f'"{col}"' for col in filtered_data[0].keys()])
                value_placeholders = ", ".join(["%s"] * len(filtered_data[0]))
                insert_query = f"""
                INSERT INTO {SNOWFLAKE_CONFIG['database']}.{SNOWFLAKE_CONFIG['schema']}.{table_name} 
                ({column_list}) VALUES ({value_placeholders})
                """

                cursor.executemany(insert_query, [tuple(record.values()) for record in filtered_data])
                conn.commit()
                log_info(f"✅ {len(filtered_data)} new records inserted into table: {table_name}")

            finally:
                cursor.close()
                conn.close()

    except Exception as e:
        log_error(f"❌ Error processing CDC data: {e}")

def main():
    """Main function to run the CDC pipeline."""
    try:
        download_and_process_blob()
    except Exception as e:
        log_error(f"❌ Error in main(): {str(e)}")

if __name__ == "__main__":
    main()

