
# Airport Data Warehouse ETL pipeline

This project implements an ETL pipeline to build an analytical database for an airport's data.

Raw operational data is extracted from source databases, transformed using a custom multidimensional schema and loaded into a DuckDB data warehouse optimized for analytical queries and KPI computation.

The final system enables faster and simpler access to key airport performance indicators compared to querying the original operational databases.

ETL flow: Extract → Transform → Load

Tech stack: Python, pygrametl, psycopg2, DuckDB
