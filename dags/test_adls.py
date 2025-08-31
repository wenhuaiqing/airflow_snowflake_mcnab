# # /usr/local/airflow/dags/test_adls.py
# import os
# from adlfs import AzureBlobFileSystem
# from dotenv import load_dotenv

# # Load environment variables from .env
# load_dotenv()

# AZURE_STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT")
# AZURE_STORAGE_KEY = os.getenv("AZURE_STORAGE_KEY")
# AZURE_CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME")
# INGEST_FOLDER_NAME = os.getenv("INGEST_FOLDER_NAME")

# print("Account:", AZURE_STORAGE_ACCOUNT)
# print("Key length:", len(AZURE_STORAGE_KEY))

# # Create AzureBlobFileSystem instance
# fs = AzureBlobFileSystem(account_name=AZURE_STORAGE_ACCOUNT, account_key=AZURE_STORAGE_KEY)

# # Full path to folder
# path = f"{AZURE_CONTAINER_NAME}/{INGEST_FOLDER_NAME}"
# print(f"Listing files in abfs://{path} ...")

# try:
#     files = fs.ls(path)
#     for f in files:
#         print(f)
# except Exception as e:
#     print("Failed to list files:", e)


# import pandas as pd
# from adlfs import AzureBlobFileSystem
# import os
# from dotenv import load_dotenv

# load_dotenv()

# fs = AzureBlobFileSystem(
#     account_name=os.getenv("AZURE_STORAGE_ACCOUNT"),
#     account_key=os.getenv("AZURE_STORAGE_KEY")
# )

# remote_file = f"{os.getenv('AZURE_CONTAINER_NAME')}/{os.getenv('INGEST_FOLDER_NAME')}/contractors.csv"

# with fs.open(remote_file, "rb") as f:
#     df = pd.read_csv(f)

# print(df.head())


# test_df = df.head(2)  # just a small sample
# remote_test_file = f"{os.getenv('AZURE_CONTAINER_NAME')}/{os.getenv('INGEST_FOLDER_NAME')}/test_upload.csv"

# with fs.open(remote_test_file, "wb") as f:
#     test_df.to_csv(f, index=False)

# print("Uploaded test_upload.csv")