import os
import pandas as pd
from sqlalchemy import create_engine

# 1. Database connection configuration (matches your docker-compose.yml)
# Format: postgresql://username:password@host:exposed_port/database_name
DB_URL = "postgresql://admin:admin123@localhost:55433/olist_db"
engine = create_engine(DB_URL)

# 2. Relative path pointing to your raw data folder
RAW_DATA_DIR = "data/raw"

def ingest_data():
    print("Starting to ingest Olist dataset into PostgreSQL...")
    
    # Get all CSV files in the directory
    csv_files = [f for f in os.listdir(RAW_DATA_DIR) if f.endswith('.csv')]
    
    if not csv_files:
        print("Error: No CSV files found in the data/raw directory. Please check!")
        return

    for filename in csv_files:
        file_path = os.path.join(RAW_DATA_DIR, filename)
        
        # Clean table names: remove 'olist_' prefix and '_dataset' suffix for better business readability
        # Example: olist_orders_dataset.csv -> orders
        table_name = filename.replace("olist_", "").replace("_dataset.csv", "").replace(".csv", "")
        
        print(f"Reading {filename} -> Preparing to write to table [{table_name}]...")
        
        # Read CSV, automatically infer data types
        df = pd.read_csv(file_path)
        
        # Write data to PostgreSQL
        # if_exists='replace' means it will overwrite existing tables every time the script runs, making debugging easier
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
        
        print(f"Table [{table_name}] successfully written! Total {len(df)} rows.\n")

    print("Congratulations! All business data has been successfully loaded into the OLTP database.")

if __name__ == "__main__":
    ingest_data()
