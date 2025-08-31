import os
from dotenv import load_dotenv
import snowflake.connector

# Load .env file from the project root
load_dotenv()

# Read credentials from environment variables
USER = os.getenv("SNOWFLAKE_USER")
PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
DATABASE = os.getenv("SNOWFLAKE_DB_NAME", "ETL_DEMO")
SCHEMA = os.getenv("SNOWFLAKE_SCHEMA_NAME", "DEV")

print("Using credentials:")
print(f"USER={USER}, ACCOUNT={ACCOUNT}, WAREHOUSE={WAREHOUSE}")

try:
    conn = snowflake.connector.connect(
        user=USER,
        password=PASSWORD,
        account=ACCOUNT,
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA
    )
    cs = conn.cursor()
    cs.execute("SELECT current_version()")
    one_row = cs.fetchone()
    print("Snowflake connection successful! Version:", one_row[0])
    cs.close()
    conn.close()
except Exception as e:
    print("Connection failed:", e)