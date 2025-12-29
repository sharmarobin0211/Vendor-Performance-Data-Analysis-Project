# ============================================================
# MySQL CSV Ingestion Script (Fast Version)
# ============================================================
# This script reads all CSV files from the 'data/' folder
# and loads them into a MySQL database using the
# super-fast 'LOAD DATA LOCAL INFILE' method.
# Each CSV filename will be used as the table name.
# ============================================================

import pandas as pd
import os
import logging
import time
import mysql.connector

# ------------------------------------------------------------
# 🪵 Logging Configuration
# ------------------------------------------------------------
# Create a folder named 'logs' if it doesn't exist
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename="logs/ingestion_db_fast.log",  # Log file location
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# ------------------------------------------------------------
#  MySQL Database Connection Settings
# ------------------------------------------------------------
username = "root"             #  your MySQL username
password = "Sudhu@123"    #  your MySQL password
host = "127.0.0.1"            # or AWS RDS endpoint
port = 3306                   # default MySQL port
database = "inventory"        # your database name

# ------------------------------------------------------------
#  Function: Ingest CSV using MySQL's LOAD DATA INFILE
# ------------------------------------------------------------
def ingest_db_fast(csv_path, table_name, conn):
    """
    Loads CSV directly into MySQL using LOAD DATA INFILE.
    - Automatically creates a table if it doesn't exist.
    - Loads millions of rows in minutes.
    """
    cursor = conn.cursor()
    try:
        # Read just the header to auto-create table columns
        df_head = pd.read_csv(csv_path, nrows=5)
        columns = ", ".join([f"`{col}` TEXT" for col in df_head.columns])
        create_sql = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({columns});"
        cursor.execute(create_sql)
        conn.commit()

        # Prepare SQL command for bulk load
        abs_path = os.path.abspath(csv_path).replace("\\", "/")  # convert path for Windows
        load_sql = f"""
        LOAD DATA LOCAL INFILE '{abs_path}'
        INTO TABLE `{table_name}`
        FIELDS TERMINATED BY ','
        ENCLOSED BY '"'
        LINES TERMINATED BY '\\n'
        IGNORE 1 ROWS;
        """


        
        logging.info(f" Loading {csv_path} into table `{table_name}`")
        
        # Track loading time
        start_time = time.time()
        cursor.execute(load_sql)
        conn.commit()
        end_time = time.time()
        
        logging.info(f"Table `{table_name}` loaded successfully in {(end_time - start_time)/60:.2f} minutes")

    except Exception as e:
        logging.error(f" Error loading {csv_path}: {str(e)}")

    finally:
        cursor.close()

# ------------------------------------------------------------
#  Load all CSV files from the /data folder
# ------------------------------------------------------------
def load_raw_data():
    start = time.time()
    data_folder = "data"

    # Check folder existence
    if not os.path.exists(data_folder):
        logging.error(" 'data' folder not found. Please create it and add CSV files.")
        return

    csv_files = [f for f in os.listdir(data_folder) if f.endswith(".csv")]
    if not csv_files:
        logging.warning(" No CSV files found in data folder.")
        return

    try:
        #  Connect to MySQL
        conn = mysql.connector.connect(
            user=username,
            password=password,
            host=host,
            port=port,
            database=database,
            allow_local_infile=True  #  Required for LOAD DATA LOCAL
        )

        #  Ingest each CSV one by one
        for file in csv_files:
            csv_path = os.path.join(data_folder, file)
            table_name = file[:-4]  # remove ".csv"
            ingest_db_fast(csv_path, table_name, conn)

        conn.close()

    except Exception as e:
        logging.error(f" Database connection error: {str(e)}")

    end = time.time()
    total_time = (end - start) / 60
    logging.info("-------------- Ingestion Complete --------------")
    logging.info(f"⏱ Total Time Taken: {total_time:.2f} minutes")

# ------------------------------------------------------------
# Run the Script
# ------------------------------------------------------------
if __name__ == "__main__":
    load_raw_data()
