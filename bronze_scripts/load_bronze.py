import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import configparser
import logging
import sys
import time
import os

def load_bronze():

    os.makedirs("logs", exist_ok=True)

    # ==========================================
    # Logging Configuration
    # ==========================================
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("logs/bronze_load.log", mode="a")
        ]
    )

    # ==========================================
    # Load Snowflake Credentials
    # ==========================================
    try:
        config = configparser.ConfigParser()
        config.read("/usr/local/airflow/config/credentials.conf")

        sf_params = {
            "user": config["SNOWFLAKE"]["user"],
            "password": config["SNOWFLAKE"]["password"],
            "account": config["SNOWFLAKE"]["account"],
            "warehouse": config["SNOWFLAKE"]["warehouse"],
            "database": config["SNOWFLAKE"]["database"],
            "schema": config["SNOWFLAKE"]["bronze_schema"],
            "role": config["SNOWFLAKE"]["role"]
        }

        logging.info("Credentials loaded successfully.")

    except Exception as e:
        logging.error(f"Error reading credentials: {e}")
        sys.exit(1)

    # ==========================================
    # Create Snowflake Connection
    # ==========================================
    try:
        ctx = snowflake.connector.connect(
            user=sf_params["user"],
            password=sf_params["password"],
            account=sf_params["account"],
            warehouse=sf_params["warehouse"],
            database=sf_params["database"],
            schema=sf_params["schema"],
            role=sf_params["role"]
        )
        logging.info("Snowflake connection created successfully.")

    except Exception as e:
        logging.error(f"Failed to connect to Snowflake: {e}")
        sys.exit(1)

    # ==========================================
    # Helper Function
    # ==========================================
    def load_csv_from_github(raw_url):
        return pd.read_csv(raw_url)

    # ==========================================
    # Tables to Load
    # ==========================================
    tables_to_load = {
        "crm_cust_info": "https://raw.githubusercontent.com/DataWithBaraa/sql-data-warehouse-project/main/datasets/source_crm/cust_info.csv",
        "crm_prd_info": "https://raw.githubusercontent.com/DataWithBaraa/sql-data-warehouse-project/main/datasets/source_crm/prd_info.csv",
        "crm_sales_details": "https://raw.githubusercontent.com/DataWithBaraa/sql-data-warehouse-project/main/datasets/source_crm/sales_details.csv",
        "erp_cust_az12": "https://raw.githubusercontent.com/DataWithBaraa/sql-data-warehouse-project/main/datasets/source_erp/CUST_AZ12.csv",
        "erp_loc_a101": "https://raw.githubusercontent.com/DataWithBaraa/sql-data-warehouse-project/main/datasets/source_erp/LOC_A101.csv",
        "erp_px_cat_g1v2": "https://raw.githubusercontent.com/DataWithBaraa/sql-data-warehouse-project/main/datasets/source_erp/PX_CAT_G1V2.csv"
    }

    # ==========================================
    # Main Loading Loop
    # ==========================================
    logging.info("Starting Bronze layer loading...")

    total_start = time.perf_counter()

    for table_name, url in tables_to_load.items():
        logging.info(f"Starting load for table: {table_name}")
        
        try:
            df = load_csv_from_github(url)
            df.columns = [col.upper() for col in df.columns]
            logging.info(f"Data fetched from GitHub: {url} | Rows in CSV: {len(df)}")
            
            start = time.perf_counter()
            
            success, nchunks, nrows, _ = write_pandas(ctx, df, table_name, overwrite=True)
            if success:
                logging.info(f"Successfully loaded table: {table_name} | Rows inserted: {nrows}")
            else:
                logging.error(f"Failed to load table: {table_name}")
            
            duration = time.perf_counter() - start
            logging.info(f"{table_name} loaded in {duration:.2f} seconds.")
            
        except Exception as e:
            logging.error(f"Failed to load table {table_name}: {e}")
            continue

    ctx.close()
    logging.info("All Bronze tables processed.")