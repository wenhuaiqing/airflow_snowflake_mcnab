# Azure Data Lake Gen2 + Snowflake + Astro Deployment Checklist

This checklist covers everything needed to deploy your ETL pipeline with Azure Data Lake Gen2, Snowflake, and Astro.


## 🔧 Azure Data Lake Gen2 Setup Required

### 1. Create Azure Storage Account
```bash
# Create resource group (if needed)
az group create --name rg-construction-etl --location eastus

# Create storage account with hierarchical namespace enabled
az storage account create     --name <your-storage-account-name>     --resource-group rg-construction-etl     --location eastus     --sku Standard_LRS     --enable-hierarchical-namespace true
```

### 2. Create Container and Folders
```bash
# Create container
az storage fs create     --name construction-data     --account-name <your-storage-account-name>

# Create folder structure
az storage fs directory create     --name construction-ingest     --file-system construction-data     --account-name <your-storage-account-name>

az storage fs directory create     --name construction-stage     --file-system construction-data     --account-name <your-storage-account-name>

az storage fs directory create     --name construction-archive     --file-system construction-data     --account-name <your-storage-account-name>
```

### 3. Create Service Principal for Authentication
```bash
# Create service principal
az ad sp create-for-rbac --name "airflow-construction-etl" --role "Storage Blob Data Contributor" --scopes "/subscriptions/<subscription-id>/resourceGroups/rg-construction-etl/providers/Microsoft.Storage/storageAccounts/<storage-account-name>"
```

Save the output values:
- `appId` → AZURE_CLIENT_ID
- `password` → AZURE_CLIENT_SECRET  
- `tenant` → AZURE_TENANT_ID

## ❄️ Snowflake Setup Required

### 1. Create Database and Schema
```sql
-- Create warehouse
CREATE WAREHOUSE IF NOT EXISTS CONSTRUCTION_WH 
WITH WAREHOUSE_SIZE = 'XSMALL' 
AUTO_SUSPEND = 60 
AUTO_RESUME = TRUE;

-- Create database and schema
CREATE DATABASE IF NOT EXISTS ETL_DEMO;
CREATE SCHEMA IF NOT EXISTS ETL_DEMO.DEV;

-- Create role
CREATE ROLE IF NOT EXISTS construction_etl_role;

-- Grant permissions
GRANT USAGE ON WAREHOUSE CONSTRUCTION_WH TO ROLE construction_etl_role;
GRANT USAGE ON DATABASE ETL_DEMO TO ROLE construction_etl_role;
GRANT USAGE ON SCHEMA ETL_DEMO.DEV TO ROLE construction_etl_role;
GRANT ALL PRIVILEGES ON SCHEMA ETL_DEMO.DEV TO ROLE construction_etl_role;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ETL_DEMO.DEV TO ROLE construction_etl_role;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA ETL_DEMO.DEV TO ROLE construction_etl_role;
GRANT ALL PRIVILEGES ON ALL STAGES IN SCHEMA ETL_DEMO.DEV TO ROLE construction_etl_role;
GRANT ALL PRIVILEGES ON FUTURE STAGES IN SCHEMA ETL_DEMO.DEV TO ROLE construction_etl_role;
```

### 2. Create User and Assign Role
```sql
-- Create user
CREATE USER IF NOT EXISTS construction_etl_user
    PASSWORD = '<your-password>'
    DEFAULT_ROLE = construction_etl_role
    MUST_CHANGE_PASSWORD = FALSE;

-- Grant role to user
GRANT ROLE construction_etl_role TO USER construction_etl_user;
```

### 3. Create External Stage for Azure Data Lake Gen2
```sql
USE DATABASE ETL_DEMO;
USE SCHEMA DEV;

-- Create stage pointing to Azure Data Lake Gen2
CREATE OR REPLACE STAGE CONSTRUCTION_STAGE
URL = 'azure://<your-storage-account>.dfs.core.windows.net/construction-data/construction-stage/'
CREDENTIALS = (
    AZURE_CLIENT_ID = '<your-client-id>'
    AZURE_CLIENT_SECRET = '<your-client-secret>'
    AZURE_TENANT_ID = '<your-tenant-id>'
)
FILE_FORMAT = (TYPE = 'CSV', FIELD_DELIMITER = ',', SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"');
```

## 🚀 Astro Deployment

### 1. Environment Variables to Set in Astro
```bash
# Azure Configuration
AZURE_CONN_ID=azure_default
AZURE_STORAGE_ACCOUNT=<your-storage-account-name>
AZURE_CONTAINER_NAME=construction-data
AZURE_TENANT_ID=<your-tenant-id>
AZURE_CLIENT_ID=<your-client-id>
AZURE_CLIENT_SECRET=<your-client-secret>

# Azure Folder Names
INGEST_FOLDER_NAME=construction-ingest
STAGE_FOLDER_NAME=construction-stage
ARCHIVE_FOLDER_NAME=construction-archive

# Snowflake Configuration
SNOWFLAKE_CONN_ID=snowflake_default
SNOWFLAKE_DB_NAME=ETL_DEMO
SNOWFLAKE_SCHEMA_NAME=DEV
SNOWFLAKE_STAGE_NAME=CONSTRUCTION_STAGE
SNOWFLAKE_ACCOUNT=<your-account>.snowflakecomputing.com
SNOWFLAKE_USER=construction_etl_user
SNOWFLAKE_PASSWORD=<your-password>
SNOWFLAKE_ROLE=construction_etl_role
SNOWFLAKE_WAREHOUSE=CONSTRUCTION_WH

# Slack (Optional)
SLACK_CONN_ID=slack_conn
SLACK_WEBHOOK_URL=<your-webhook-url>
```

### 2. Airflow Connections to Create in Astro UI

#### Azure Data Lake Connection
- **Connection ID**: `azure_default`
- **Connection Type**: `Azure Data Lake`
- **Extra**: 
```json
{
    "tenant_id": "<your-tenant-id>",
    "client_id": "<your-client-id>",
    "client_secret": "<your-client-secret>",
    "account_name": "<your-storage-account-name>"
}
```

#### Snowflake Connection
- **Connection ID**: `snowflake_default`
- **Connection Type**: `Snowflake`
- **Host**: `<your-account>.snowflakecomputing.com`
- **Login**: `construction_etl_user`
- **Password**: `<your-password>`
- **Schema**: `DEV`
- **Extra**:
```json
{
    "database": "ETL_DEMO",
    "warehouse": "CONSTRUCTION_WH",
    "role": "construction_etl_role",
    "account": "<your-account>"
}
```

#### Slack Connection (Optional)
- **Connection ID**: `slack_conn`
- **Connection Type**: `Slack Webhook`
- **Password**: `<your-webhook-url>`

### 3. Deploy to Astro
```bash
# Login to Astro
astro auth login

# Deploy to your deployment
astro deploy
```

## 🧪 Testing Checklist

### Local Testing
- [ ] Run `astro dev start` locally
- [ ] Verify all connections work
- [ ] Test extract_from_api DAG
- [ ] Test move_ingest_to_stage DAG
- [ ] Test load_to_snowflake DAG

### Production Testing
- [ ] Deploy to Astro
- [ ] Verify environment variables are set
- [ ] Test Azure Data Lake Gen2 connectivity
- [ ] Test Snowflake connectivity
- [ ] Run full pipeline end-to-end
- [ ] Verify data quality checks
- [ ] Test Slack notifications (if enabled)

## 🚨 Common Issues and Solutions

### Issue: "Connection not found"
- **Solution**: Ensure connection IDs match environment variables and are created in Airflow UI

### Issue: "Permission denied" on Azure
- **Solution**: Verify Service Principal has "Storage Blob Data Contributor" role

### Issue: "Stage not found" in Snowflake
- **Solution**: Ensure Snowflake stage is created and points to correct Azure path

### Issue: "File format error" in Snowflake
- **Solution**: Verify CSV format settings match your data structure

## 📋 Final Verification

Before going live, verify:
- [ ] All DAGs are unpaused and running successfully
- [ ] Data flows from API → Azure → Snowflake
- [ ] Data quality checks are passing
- [ ] Notifications are working (if enabled)
- [ ] All tables are being created and populated correctly
- [ ] Archival process is working (if implemented)

## 🎯 Next Steps

1. Set up monitoring and alerting
2. Implement data lineage tracking
3. Add more comprehensive data quality checks
4. Set up automated testing
5. Consider implementing CI/CD pipeline
