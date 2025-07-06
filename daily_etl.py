import os
import pandas as pd
import sqlite3
import re

# Path to the folder where all CSVs live
folder_path = r"C:\Users\singl\Desktop\data_lake_project\raw_data"

# SQLite database path
db_path = r"C:\Users\singl\Desktop\data_lake_project\processed_data.db"

# Connect to SQLite
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# 1️⃣ Truncate tables before loading
cursor.execute("DELETE FROM CUST_MSTR")
cursor.execute("DELETE FROM MASTER_CHILD")
cursor.execute("DELETE FROM H_ECOM_ORDER")
conn.commit()

# 2️⃣ Process files in folder
for file in os.listdir(folder_path):
    file_path = os.path.join(folder_path, file)

    # CUST_MSTR
    if file.lower().startswith("cust_mstr"):
        # extract date
        match = re.search(r"(\d{8})", file)
        if match:
            date_raw = match.group(1)
            date_formatted = f"{date_raw[:4]}-{date_raw[4:6]}-{date_raw[6:]}"
        else:
            continue  # skip if no date found

        df = pd.read_csv(file_path)
        df["Date"] = date_formatted
        df.to_sql("CUST_MSTR", con=conn, if_exists="append", index=False)
        print(f"Loaded {file} into CUST_MSTR with date {date_formatted}")

    # MASTER_CHILD
    elif file.lower().startswith("master_child_export"):
        match = re.search(r"(\d{8})", file)
        if match:
            date_raw = match.group(1)
            date_formatted = f"{date_raw[:4]}-{date_raw[4:6]}-{date_raw[6:]}"
            date_key = int(date_raw)
        else:
            continue  # skip if no date found

        df = pd.read_csv(file_path)
        df["Date"] = date_formatted
        df["DateKey"] = date_key
        df.to_sql("MASTER_CHILD", con=conn, if_exists="append", index=False)
        print(f"Loaded {file} into MASTER_CHILD with date {date_formatted} and DateKey {date_key}")

    # H_ECOM_ORDER
    elif file.lower().startswith("h_ecom_order"):
        df = pd.read_csv(file_path)
        df.to_sql("H_ECOM_ORDER", con=conn, if_exists="append", index=False)
        print(f"Loaded {file} into H_ECOM_ORDER")

conn.close()
print("✅ ETL completed successfully.")
