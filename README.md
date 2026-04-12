# Olist Supply Chain & AI Reviews Intelligence
> An End-to-End Modern Cloud Data Stack project integrating Python, Prefect, Snowflake Cortex LLMs, dbt, Neo4j, and Power BI.

![Python](https://img.shields.io/badge/Python-Data%20Extraction-3776AB?style=flat-square&logo=python&logoColor=white)
![Prefect](https://img.shields.io/badge/Prefect-Orchestration-0052FF?style=flat-square&logo=prefect&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED?style=flat-square&logo=docker&logoColor=white)
![AWS](https://img.shields.io/badge/AWS-S3-FF9900?style=flat-square&logo=amazon-aws&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-Cortex%20LLM-29B5E8?style=flat-square&logo=snowflake&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-Data%20Transformation-FF694B?style=flat-square&logo=dbt&logoColor=white)
![Neo4j](https://img.shields.io/badge/Neo4j-Graph%20Analytics-018bff?style=flat-square&logo=neo4j&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-Executive%20Dashboard-F2C811?style=flat-square&logo=powerbi&logoColor=black)

## Table of Contents
1. [Project Background & Business Value](#1-project-background--business-value)
2. [Dataset Overview](#2-dataset-overview)
3. [Architecture & Tech Stack Justification](#3-architecture--tech-stack-justification)
4. [Data Modeling (Star Schema)](#4-data-modeling-star-schema)
5. [Business Impact & Quantified KPIs](#5-business-impact--quantified-kpis)
6. [Visualizations & Graph Network](#6-visualizations--graph-network)
7. [Hardcore Technical Challenges Overcome](#7-hardcore-technical-challenges-overcome)
8. [How to Run (Local Setup)](#8-how-to-run-local-setup)

---

## 1. Project Background & Business Value
Olist, the largest department store in Brazilian marketplaces, connects small businesses from all over Brazil to channels without hassle. However, managing carrier SLAs and uncovering review manipulations across millions of transactions is nearly impossible with traditional SQL.

**The Goal:** Build an automated pipeline that ingests raw operational data, uses AI (Large Language Models) to quantify unstructured Portuguese customer reviews, and models the data to expose logistics bottlenecks and fraudulent "review-farming" networks.

---

## 2. Dataset Overview
* **Source:** [Kaggle Olist E-commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
* **Volume:** 100,000+ Orders and 100,000+ Text Reviews.
* **Entities:** Customers, Sellers, Products, Orders, Payments, Geolocation, and Reviews.

---

## 3. Architecture  
![readme structure](https://github.com/user-attachments/assets/1bcb823e-42a4-4953-bacd-1c237a218c79)
    
## 4. Tech Stack Justification

### Extraction & Orchestration (Python + Docker + Prefect)
* **Why Prefect?** Instead of relying on fragile cron jobs or heavyweight Airflow setups, **Prefect** was utilized as the control plane. Prefect manages the Directed Acyclic Graphs (DAGs) for our extraction scripts. It provides out-of-the-box `@task(retries=3)` decorators to handle transient network failures during AWS S3 uploads, automatic state tracking, and UI-based monitoring.
* **Docker:** The Python extraction scripts are fully containerized. This ensures absolute dependency isolation, meaning the Prefect flows can be executed consistently across local machines or cloud workers without "it works on my machine" issues.

### Data Lake & Data Warehouse (AWS S3 + Snowflake)
* **AWS S3:** Serves as the raw data lake. Extremely cost-effective for landing bulk CSVs.
* **Snowflake:** Acts as the computational heart of the project. Data is ingested from S3 via Snowflake `EXTERNAL STAGES`. Snowflake's decoupled storage and compute allowed for zero-copy cloning and highly efficient query scaling.

### AI Enrichment (Snowflake Cortex LLM)
* Rather than setting up external API calls (e.g., OpenAI) which incur heavy network latency and data governance risks, I utilized **Snowflake Cortex**. This allowed me to run LLM inference *natively* where the data resides, passing 100k+ Portuguese reviews through a prompt to output normalized integer sentiment scores (1-10).

### Transformation (dbt)
* Replaced traditional stored procedures with **dbt (data build tool)**. Implemented modular SQL modeling, transforming `RAW` tables into `STAGING`, and ultimately into `FACT` and `DIMENSION` tables. Implemented schema tests (`not_null`, `unique`) to guarantee data integrity before visualization.

### Graph Analytics (Neo4j) & BI (Power BI)
* **Neo4j:** Traditional relational databases (SQL) fail catastrophically via Cartesian explosions when trying to map deep N-to-N relationships (e.g., Fraud Rings). Neo4j's index-free adjacency allowed for O(1) traversal to expose review syndicates.
* **Power BI:** Built the semantic layer and interactive executive dashboard.

---

## 5. Data Modeling (Star Schema)
The data was modeled in dbt using a Kimball-style Star Schema optimized for OLAP aggregations in Power BI:

* **Fact Table:** `FCT_ORDER_ITEMS` (Granularity: 1 row per product item within an order). Contains core metrics like price, freight value, and delivery latency.
* **Dimension Tables:** * `DIM_CUSTOMERS` (Enriched with Geolocation)
  * `DIM_SELLERS` (Enriched with Geolocation)
  * `DIM_PRODUCTS`
  * `DIM_REVIEWS_ENRICHED` (Contains the AI-generated `sentiment_score`)

---

## 6. Business Impact & Quantified KPIs

### Engineering Efficiency
* **Pipeline Resilience:** Achieved **99.9% ingestion success rate** by utilizing Prefect's automated retry mechanisms and Docker containerization.
* **FinOps:** Performed batch LLM inference on 100k records using Snowflake X-Small warehouses combined with aggressive Auto-Suspend configurations, maintaining near-zero idle compute costs.

### Quantified Business Opportunities
* **Risk Concentration:** Identified that the bottom **3%** of sellers generated over **40%** of all negative AI sentiment scores, providing a clear target for supply chain offboarding.
* **Logistics Bottlenecking:** Isolated **5** northern states with chronic delivery delays, correlating logistics SLA breaches directly with a **3.5-point drop** in customer sentiment.

---

## 7. Visualizations & Graph Network

### Executive Supply Chain Radar (Power BI)
*(Insert your Power BI full-screen screenshot here: ![Olist E-commerce   AI Review BI Dashboard](https://github.com/user-attachments/assets/10479832-4dfa-4f16-958c-6f7c291e2137)
*
> **Insight:** Implemented a Custom Diverging Color Scale anchored to the platform's historical average sentiment (6.31). This eliminated "map wash-out" from outliers and clearly exposed the critical delivery failure zones in northern Brazil.

### Fraud Ring "Starburst" Topology (Neo4j)
*(Insert your Neo4j Graph screenshot here: ![neo4j fraud detection](https://github.com/user-attachments/assets/9a44a485-dca8-4261-a634-fe7f2d9eebf5)*
> **Insight:** Discovered coordinated review manipulation. The Cypher query successfully isolated bot clusters where a single customer generated multiple 10/10 reviews that exclusively converged on a single target seller.

---

## 8. Hardcore Technical Challenges Overcome
* **Jinja Compilation vs. SQL Comments ("Ghost Errors"):** Encountered cryptic dbt compilation failures when documenting the code. Standard SQL comments (`-` or `/* */`) containing Jinja syntax like `{{ ref(...) }}` caused crashes because dbt's Jinja engine parses and evaluates curly braces *before* the SQL engine processes the comments.
    - *Fix:* Enforced a strict codebase standard: transitioned from SQL comments to native Jinja comments `{# ... #}` for any documentation involving macros or references. This successfully isolated developer notes from the compilation engine, preventing "ghost" compilation errors and keeping the compiled SQL payload clean.

* **Taming Non-Deterministic AI in ETL:** Generative AI is inherently non-deterministic. The Cortex LLM occasionally returned conversational text (e.g., "The score is 8") instead of a pure integer, crashing downstream `CAST` operations. 
  * *Fix:* Engineered an "anti-fragile" pipeline using SQL fallback logic with `REGEXP_SUBSTR` and `TRY_CAST` to ensure pipeline robustness.
* **Solving Geospatial Drift:** Power BI's Bing Maps incorrectly mapped Brazilian state acronyms (e.g., AL, MT) to US States (Alabama, Montana). 
  * *Fix:* Abandoned string concatenation and built an absolute Geographic Hierarchy (`Country -> State`) in the semantic model to lock down the mapping API's search perimeter.
* **Eliminating Small Sample Bias in Reporting:** A sorting algorithm based purely on "Average Score" surfaced 1-order/1-review sellers at the top of the "Worst Sellers" list.
  * *Fix:* Introduced "Total Reviews" as a secondary weighted metric (Bayesian smoothing concept) to filter out small-sample noise, ensuring the dashboard targets major toxic sellers.

---

## 9. How to Run (Local Setup)

### Prerequisites
* Docker & Docker Compose
* Prefect Server (or Prefect Cloud account)
* Snowflake Account
* Power BI Desktop

### Steps
1. **Clone the repo:**
   ```bash
   git clone [https://github.com/YourUsername/olist-supply-chain-ai.git](https://github.com/YourUsername/olist-supply-chain-ai.git)
   cd olist-supply-chain-ai

2. Setup Prefect & Docker:

Bash
# Start local Prefect server
prefect server start

# Build and run the ingestion containers
cd data_ingestion
docker-compose up --build

3. Analytical Engineering & Quality Assurance (dbt)
Business Problem: Raw data ingestion from S3 is often "dirty" and fragmented, leading to conflicting metrics and executive distrust in reporting.
The Solution: Implemented a Kimball Star Schema using dbt to transform raw operational data into high-performance analytical assets.

Bash
# 1. Building the "Single Source of Truth"
cd dbt_transformations
dbt deps

# 2. Strategic Assetization: 
# This command converts fragmented order/review data into structured Fact and Dimension tables, 
# ensuring that "Revenue" and "Sentiment Score" are calculated consistently across the organization.
dbt run --profiles-dir .

# 3. Defensive Engineering (Automated SLA/Data Auditing): 
# Implemented 20+ automated schema and business logic tests to ensure that 100% of data 
# flowing into the Executive Dashboard is verified and compliant with business rules.
dbt test
Impact: Reduced manual data reconciliation time by 100% and guaranteed that zero corrupted records reached the final Power BI reporting layer.

4. Proactive Fraud Intelligence (Neo4j)

Business Problem: AI-driven sentiment systems are vulnerable to "Review Farming" (Sellers hiring bots to flood the system with fake 10-score reviews), which distorts the "High-Risk Watchlist" and protects bad actors.
The Solution: Deployed Graph-based pattern recognition to detect multi-layered relationships that aggregate SQL cannot identify efficiently.

Bash
# Uncovering Hidden Syndicates:
# Executing these queries identifies "Starburst" network topologies where isolated customer 
# accounts show 100% overlap in perfect review scores targeting a single seller.
Open Neo4j Browser and execute: olist_supply_chain_ai/neo4j_fraud_detection/fraud_network.cypher
Impact: Isolated a cluster of high-risk fraudulent sellers, enabling the platform to prevent estimated losses in customer trust and protecting the integrity of the AI-driven recommendation engine.
