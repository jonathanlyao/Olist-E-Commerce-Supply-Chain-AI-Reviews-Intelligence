LOAD CSV WITH HEADERS FROM "file:///olist_customers.csv" AS row CREATE (:Customer {customer_id: row.customer_id})

// Core Cypher Query: Detecting Coordinated Review Syndicates
MATCH (c:Customer)-[:WROTE]->(r:Review)-[:REVIEWS]->(p:Product)-[:SOLD_BY]->(s:Seller)
WHERE r.sentiment_score >= 9.0
WITH c, count(DISTINCT s) AS distinct_sellers, count(r) AS total_perfect_reviews

// Core Logic: The customer left more than 5 perfect reviews, but ALL of them are exclusively for ONE single seller.
WHERE total_perfect_reviews > 5 AND distinct_sellers = 1 
RETURN c.customer_id, total_perfect_reviews
ORDER BY total_perfect_reviews DESC


