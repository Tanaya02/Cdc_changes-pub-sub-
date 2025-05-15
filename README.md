# Near Real-Time CDC Streaming: SQL Server to Snowflake

A robust solution for streaming Change Data Capture (CDC) data from SQL Server to Snowflake in near real-time using a publisher-subscriber architecture.

## Overview

This project provides an enterprise-ready data integration platform that continuously captures changes (inserts, updates, and deletes) from SQL Server databases and replicates them to Snowflake with sub-minute latency. The publisher-subscriber architecture with message queuing ensures reliable data transfer without impacting operational database performance.

## Architecture

```
SQL Server → CDC Reader/Publisher → Message Queue → CDC Consumer/Subscriber → Snowflake
```

### Key Components:
- **SQL Server (Source)**: With CDC enabled on tables of interest
- **Publisher**: Extracts CDC data and publishes to queue
- **Message Queue**: Buffers data for reliable processing
- **Azure Blob Storage**: Provides durable backup for CDC data
- **Subscriber**: Processes queue messages and loads to Snowflake
- **Orchestrator**: Manages execution of publisher and subscriber processes

## Features

- **Near Real-Time Sync**: Sub-minute latency data replication
- **Schema Evolution**: Automatic handling of schema changes
- **Reliable Processing**: Durable storage and error recovery mechanisms
- **Deduplication**: Prevents duplicate record processing
- **Monitoring**: Comprehensive logging and tracking system

## Installation

### Prerequisites
- Python 3.7+
- SQL Server with CDC enabled on source tables
- Snowflake account with appropriate permissions
- Azure Blob Storage account

### Setup
1. Clone the repository
```bash
git clone https://github.com/Tanaya02/Cdc_changes-pub-sub-.git
cd cdc-sqlserver-snowflake
```

2. Install dependencies
```bash
pip install -r requirements.txt
```

3. Configure database connections
   - Create `config/db_config.py` with SQL Server and Snowflake credentials
   - Create `config/azure_storage.py` with Azure Blob Storage credentials

4. Enable CDC on SQL Server tables (if not already enabled)
```sql
USE YourDatabase;
EXEC sys.sp_cdc_enable_db;
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'YourTable',
    @role_name = NULL;
```

## Usage

### Running Options

**Option 1**: Continuous scheduled execution (recommended for production)
```bash
python -m continuous_runner
```
This runs the publisher every 20 seconds and the subscriber every 45 seconds.

**Option 2**: Simple parallel execution
```bash
python -m main
```
This runs the publisher and subscriber concurrently in separate processes.

### Monitoring

- Check log files for execution status and errors
- Monitor LSN tracking files (`last_lsn_*.txt`) to verify progress
- Use `print_queue_contents()` function for queue inspection

## File Structure

- `publisher.py`: Extracts CDC data from SQL Server
- `subscriber.py`: Loads data into Snowflake
- `queue_handler.py`: Manages queue operations
- `azure_blob.py`: Handles Azure Blob Storage operations
- `continuous_runner.py`: Scheduled orchestration
- `main.py`: Simple parallel execution
- `config/`: Configuration files

## Configuration

Example configurations (update with your credentials):

### SQL Server (`config/db_config.py`)
```python
DB_CONFIG = {
    "server": "your_server",
    "database": "your_database",
    "username": "your_username",
    "password": "your_password"
}
```

### Snowflake (`config/db_config.py`)
```python
SNOWFLAKE_CONFIG = {
    "user": "your_user",
    "password": "your_password",
    "account": "your_account",
    "database": "your_database",
    "schema": "your_schema"
}
```

### Azure Storage (`config/azure_storage.py`)
```python
AZURE_STORAGE_CONFIG = {
    "connection_string": "your_connection_string",
    "container_name": "your_container"
}
```

## Performance Optimization

- Adjust batch sizes based on data volume
- Tune execution frequency based on change rate
- Multiple CDC tables are processed concurrently

## Troubleshooting

- **Connection Issues**: Verify network connectivity and credentials
- **Process Overlapping**: Increase timeout value if needed (default: 5 minutes)
- **Data Inconsistencies**: Verify CDC is properly enabled on source tables
- **Memory Issues**: Adjust batch sizes for large datasets

## Security Notes

- Store credentials securely (use environment variables in production)
- Ensure proper network security for database connections
- Consider encrypting sensitive data in transit

