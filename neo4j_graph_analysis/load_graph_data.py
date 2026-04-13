import snowflake.connector
from neo4j import GraphDatabase

# ==========================================
# 1. Credentials Configuration
# ==========================================
SNOWFLAKE_USER = 'jonathansnwflk'
SNOWFLAKE_PASSWORD = 'phez3dRocR-XiqlT'
SNOWFLAKE_ACCOUNT = 'FZPFTPF-LOB40082'

NEO4J_URI = 'neo4j+s://423bc451.databases.neo4j.io'
NEO4J_USER = '423bc451'
NEO4J_PASSWORD = 'f5KwuBkYqCKguhsxl7MBWAnygqr18F3YkVxOcvOwEzY'

# ==========================================
# 2. Cypher Query Template
# Using MERGE for idempotency (UPSERT behavior)
# ==========================================
MERGE_REVIEW_CYPHER = """
    MERGE (o:Order {order_id: $order_id})
    MERGE (r:Review {review_id: $review_id})
    SET r.ai_sentiment_score = $sentiment_score
    MERGE (o)-[:HAS_REVIEW]->(r)
"""

def load_data_to_graph():
    print("Initiating Data Bridge: Snowflake -> Neo4j...")

    # Connect to Snowflake
    ctx = snowflake.connector.connect(
        user=SNOWFLAKE_USER, 
        password=SNOWFLAKE_PASSWORD, 
        account=SNOWFLAKE_ACCOUNT,         
        warehouse='COMPUTE_WH',
        database='OLIST_DB',
        schema='DBT_DEV'
    )
    cursor = ctx.cursor()

    #Connect to Neo4j
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    try:
        # Fetch a sample of 500 enriched reviews joined with their order IDs
        # We start with 500 to validate the graph model quickly and safely
        print("Fetching AI-enriched reviews from Snowflake...")
        snowflake_query = """
            SELECT
                r.review_id::VARCHAR AS review_id, 
                o.order_id::VARCHAR AS order_id, 
                r.sentiment_score
            FROM olist_db.dbt_dev.stg_reviews_enriched AS r
            JOIN olist_db.raw_data.raw_order_reviews AS o
                ON r.review_id::VARCHAR = o.review_id::VARCHAR
            WHERE r.sentiment_score IS NOT NULL
        """
        cursor.execute(snowflake_query)
        rows = cursor.fetchall()
        print(f"Successfully fetched {len(rows)} records. Pushing to Neo4j...")

        # Push data to Neo4j using a session
        with driver.session() as session: 
            count = 0
            for row in rows: 
                review_id, order_id, sentiment_score = row

                # Execute the Cypher query for each row
                session.run(
                    MERGE_REVIEW_CYPHER, 
                    order_id=order_id, 
                    review_id=review_id, 
                    sentiment_score=float(sentiment_score)
                )
                count += 1
                if count % 100 == 0: 
                    print(f"Processed {count} nodes and relationships...")
        print("Graph data ingestion completed successfully!")

    except Exception as e: 
        print(f"An error occurred: {e}")
    finally: 
        cursor.close()
        ctx.close()
        driver.close()
        print("Connections closed safely.")

if __name__ == "__main__":
    load_data_to_graph()