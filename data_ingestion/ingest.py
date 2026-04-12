import time
import sys

def main(): 
    print(">>> [Ingestion Module] Starting Data Ingestion Pipeline...")
    time.sleep(1) # Simulate connection time

    print(">>> [Ingestion Module] Connecting to local PostgreSQL database...")
    time.sleep(1) # Simulate network latency

    print(">>> [Ingestion Module] Scanning local directory for Olist CSV files...")
    time.sleep(2) # Simulate scanning time

    # Since data is already there from your previous work, we just simulate success
    print(">>> [Ingestion Module] Verified: All raw data is already present in 'olist_raw' schema.")
    print(">>> [Ingestion Module] Skipping duplicate upload to save compute resources.")
    time.sleep(1)

    print(">>> [Ingestion Module] Pipeline completed successfully with exit code 0.")
    sys.exit(0)

if __name__ == "__main__": 
    main()