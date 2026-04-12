import subprocess
from prefect import task, flow

# 1. Define the Ingestion Task
# The @task decorator tells Prefect to track this function. 
# We add retries=1 so it automatically tries again if the database connection hiccups

@task(name="Extract & Load: ingest.py", retries=1)
def run_ingestion():
    print(">>> Starting Data Ingestion...")
    # This simulates you typing 'python ingest.py' in the terminal
    result = subprocess.run(["python","ingest.py"], capture_output=True, text=True)

    if result.returncode != 0: 
        raise Exception(f"Ingestion Failed:\n{result.stderr}")

    print(">>> Ingestion Completed Successfully!")
    return True

# 2. Define the dbt Transformation Task
@task(name="Transform: dbt run")
def run_dbt():
    print(">>> Starting dbt Star Schema transformation...")
    # IMPORTANT: 'cwd' changes the working directory to where your dbt_project.yml lives
    dbt_dir = "dbt_olist"

    # This simulates you tryping 'dbt run' inside the dbt_olist folder
    result = subprocess.run(["dbt", "run"], cwd=dbt_dir, capture_output=True, text=True)

    if result.returncode != 0: 
        raise Exception(f"dbt Run Failed:\n{result.stdout}\n{result.stderr}")
    
    print(">>> dbt Completed Successfully!")

# 3. Define the Flow (The DAG Orchestrator)
# The @flow decorator is the main entry point that governs the tasks.
@flow(name="Olist Local Data Pipeline")
def olist_etl_flow(): 
    # The DAG logic is written in plain Python execution order. 
    # We use 'wait_for' to strictly enforce that dbt cannot start until ingestion finishes. 
    ingest_state = run_ingestion()
    run_dbt(wait_for=[ingest_state])

# 4. Execute the Flow
if __name__ == "__main__":
    olist_etl_flow()
