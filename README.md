# Construction Data Platform – Airflow, Snowflake, Streamlit, ADLS Gen2

This project demonstrates a modern **data engineering platform** built with:

- **Apache Airflow** for orchestration  
- **Snowflake** for data storage, transformation, and analytics  
- **Streamlit** (inside Snowflake) for interactive dashboards  
- **Azure Data Lake Storage Gen2 (ADLS Gen2)** for raw and staged data  

---

## 🚀 Project Overview

The platform ingests raw construction project data from ADLS Gen2, transforms and validates it, and loads it into Snowflake for analysis.  
End users access insights via a **Streamlit dashboard hosted in Snowflake**.  

The design ensures:

- **Scalability** – Airflow manages end-to-end data pipelines  
- **Reliability** – Data quality checks ensure trustworthy analytics  
- **Accessibility** – A self-service Streamlit app for business users  

---

## 📐 Architecture

**Flow:**  
ADLS Gen2 → Airflow DAGs → Snowflake (staging → transformed) → Streamlit dashboard  

**Key components:**
- Airflow DAGs handle **ingestion, validation, transformation, and loading**  
- Snowflake stores data in **raw, staging, and analytics schemas**  
- Streamlit provides a **no-code interface for decision-making**

![Architecture](assets/Architecture.png)

---

## ✨ Features

- Orchestrated ETL/ELT pipelines in **Airflow**
- Secure data storage in **Snowflake**
- **Data quality verification** with checksum validation
- Dashboarding via **Streamlit in Snowflake**
- Modular **SQL and Python transformations**
- Deployment-ready for cloud environments

---

## 📂 Repository Structure

```
.
├── dags/                # Airflow DAGs
│   ├── 1_ingest.py
│   ├── 2_validate.py
│   ├── 3_load_to_snowflake.py
│   └── move_stage_to_archive.py
├── include/             # Custom Python modules
│   ├── api_functions.py
│   └── snowflake_helpers.py
├── sql/                 # SQL scripts for Snowflake
│   ├── create_tables.sql
│   └── transform_data.sql
├── streamlit/           # Streamlit app (hosted inside Snowflake)
└── tests/               # Unit tests
```

---

## ⚙️ Setup

### 1. Prerequisites
- Python 3.10+
- Docker (for local Airflow)
- Snowflake account
- ADLS Gen2 container with raw files
- (Optional) [Astronomer CLI](https://www.astronomer.io/docs/astro/cli/overview) for local Airflow runtime

### 2. Environment Variables
Create a `.env` file with:

```bash
SNOWFLAKE_ACCOUNT=<account>
SNOWFLAKE_USER=<user>
SNOWFLAKE_PASSWORD=<password>
SNOWFLAKE_ROLE=<role>
SNOWFLAKE_WAREHOUSE=<warehouse>
SNOWFLAKE_DATABASE=<db>
SNOWFLAKE_SCHEMA=<schema>
ADLS_STORAGE_ACCOUNT=<account>
ADLS_CONTAINER=<container>
ADLS_SAS_TOKEN=<token>
```

### 3. Run Airflow Locally
```bash
astro dev start    # local Airflow runtime (optional)
# or docker-compose up (if using pure Airflow setup)
```

---

## 🛠️ Workflow

1. **Ingest** raw construction data from ADLS Gen2 into staging  
2. **Validate** with checksum comparison  
3. **Transform** using SQL models in Snowflake  
4. **Load** into Snowflake analytics schema  
5. **Archive** staged files post-load  
6. **Visualize** in the Streamlit dashboard inside Snowflake  

---

## 🔍 Data Quality

- **Checksum verification** between raw and staged files  
- **Schema validation** during Snowflake load  
- **Archival** of processed files ensures traceability  

---

## 📊 Streamlit in Snowflake

- The Streamlit app (`streamlit/`) is deployed **inside Snowflake**  
- Provides interactive views of projects, contractors, and budgets  
- Automatically refreshes from the analytics schema  

---

## 🚢 Deployment

- **Airflow**: Local via Docker or hosted in Astronomer/Cloud Airflow  
- **Snowflake**: Scripts in `/sql` create required objects  
- **Streamlit**: Run directly inside Snowflake for seamless access  

---

## 🧪 Testing

- Unit tests in `/tests` validate Python functions  
- DAG integrity checks ensure Airflow pipelines deploy cleanly  
- Data quality rules tested against sample datasets  

---

## ✅ Summary

This project showcases a **cloud-native data platform** integrating:

- **Airflow** – robust orchestration  
- **ADLS Gen2** – scalable raw data storage  
- **Snowflake** – secure and performant data warehouse  
- **Streamlit** – business-facing dashboards  

It provides a blueprint for production-ready ETL/ELT pipelines with strong data quality and self-service analytics.  

---

## 🚀 Deployed Streamlit App in Snowflake

The Streamlit dashboard is deployed directly inside Snowflake (Snowsight).  
👉 [Open Dashboard in Snowsight](https://app.snowflake.com/nabxrbt/sb18873/#/streamlit-apps/ETL_DEMO.DEV.MY_CONSTRUCTION_DASHBOARD?ref=snowsight_shared)

⚠️ Note: You must have a Snowflake account with the correct role and permissions to access this app. Public users without Snowflake login cannot view it.

---

## Pipeline Workflow

The pipeline is designed to be modular and decoupled, allowing for easy maintenance and scalability. The sequence of DAGs is as follows:

1. **1_extract_from_api.py**  
   - **Trigger**: Scheduled (daily).  
   - **Action**: Extracts construction data from an internal API and saves it to Azure Data Lake Storage (ADLS) in CSV format.  

2. **2_move_ingest_to_stage.py**  
   - **Trigger**: Dataset (`INGEST_DATASET`) from `1_extract_from_api.py`.  
   - **Action**: Moves the raw CSVs from the ADLS ingest folder to the stage folder.  

3. **3_load_to_snowflake.py**  
   - **Trigger**: Dataset (`STAGE_DATASET`) from `2_move_ingest_to_stage.py`.  
   - **Action**: Loads the data from the ADLS stage folder into Snowflake, applies SQL transformations, and creates a series of tables and stages.  

4. **4_transform_in_snowflake.py**  
   - **Trigger**: Dataset (`TRANSFORM_DATASET`) from `3_load_to_snowflake.py`.  
   - **Action**: Placeholder for additional transformations or enrichments in Snowflake.  

5. **5_move_stage_to_archive.py**  
   - **Trigger**: Dataset (`LOAD_COMPLETE_DATASET`) from `3_load_to_snowflake.py`.  
   - **Action**: Archives the processed files in the ADLS stage folder and moves them to the archive folder.  

### DAG Sequence Overview

1. `1_extract_from_api.py`  
2. `2_move_ingest_to_stage.py`  
3. `3_load_to_snowflake.py`  
4. `4_transform_in_snowflake.py`  
5. `5_move_stage_to_archive.py`

### DAG Sequence Diagram

![DAG Sequence Diagram](assets/dag_sequence.png)

---

## 🖼️ Screenshots

### 1. Airflow DAGs Execution
![Airflow DAGs Execution](assets/extract_from_api.png)
![Airflow DAGs Execution](assets/move_ingest_to_stage.png)
![Airflow DAGs Execution](assets/load_to_snowflake.png)
![Airflow DAGs Execution](assets/move_stage_to_archive.png)
![Airflow DAGs Execution](assets/Transform-construction-data-in-Data%20Warehouse.png)

### 2. Snowflake Tables and Stages
![Snowflake Tables](assets/Snowflake%20database%20objects.png)

### 3. ADLS Gen2 Container Files
![ADLS Gen2 Container Files](assets/ADLS%20Container.png)
![ADLS Gen2 Container Files](assets/ADLS%20Container%20Archived%20files.png)

### 4. Streamlit Deployed App
![Streamlit Dashboard in Snowflake](assets/MY_CONSTRUCTION_DASHBOARD-Snowflake-Streamlit.png)