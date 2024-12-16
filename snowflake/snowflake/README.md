
# Customer Engagement Data Workflow

### Background
The operations team maintained client engagement data in a Google Sheets file named `ClientData_2023—EngagementMetrics.csv`. This data needed to be ingested into a PostgreSQL database, cleansed, normalized, and integrated into the existing Snowflake table, `DIM_PROJECT`. The goal was to produce a comprehensive customer engagement summary table for analysis. However, the raw data required significant transformation and mapping to align with the existing data structure.

### Task
The objective was to:
1. Set up a PostgreSQL database to ingest the CSV file.
2. Cleanse and normalize the data, leveraging mapping tables to standardize inconsistent entries.
3. Load the cleaned data into Snowflake and integrate it with the `DIM_PROJECT` table to create a `customer_engagement_summary` table, enabling seamless analysis.

### Action
1. Developed a [Python script](/scripts/db_sheets_util.py) to automate the ingestion of the Google Sheets file into PostgreSQL.
2. Created two mapping tables (`word_mapping` and `number_mapping`) during the extraction process to standardize textual and numerical inconsistencies in the data.
3. Used dbt (Data Build Tool) to transform the raw data into a staging table within PostgreSQL called [stg_customer_engagement](/postgres/postgres/models/staging/stg_customer_engagement.sql), ensuring it adhered to the required format and schema.
4. Configured Airbyte to transfer the cleansed and normalized staging table from PostgreSQL to Snowflake.
5. Used dbt to join the staging data with the `DIM_PROJECT` table in Snowflake, generating the [customer_engagement_summary](/snowflake/snowflake/models/snowflake/customer_engagement_summary.sql) table.


### Result
The process successfully created a structured and cleansed `customer_engagement_summary` table in Snowflake, integrating data from both the client engagement CSV and the `DIM_PROJECT` table. This table is now readily available for analysis, providing valuable insights into customer engagement metrics while significantly improving data accuracy and accessibility.

---

## Workflow Steps

1. **Data Ingestion:**
   - A Python script is used to load the `ClientData_2023—EngagementMetrics.csv` from Google Sheets into a PostgreSQL database. 
   - During this step, two auxiliary tables, `word_mapping` and `number_mapping`, are created in PostgreSQL to assist in data cleansing.

2. **Data Transformation in PostgreSQL:**
   - The raw ingested data is cleansed and normalized using the mapping tables and transformed into a staging table via dbt.

3. **Data Loading to Snowflake:**
   - Airbyte transfers the staging table from PostgreSQL to Snowflake.

4. **Final Integration:**
   - In Snowflake, the staging table is joined with the `DIM_PROJECT` table using dbt to create the `customer_engagement_summary` table. 
   - The resulting table is now ready for analysis.

---

## Tools Used
- **Google Sheets**: Source of the raw data.
- **Python**: Script for data ingestion and preprocessing.
- **PostgreSQL**: Intermediate database for storing raw and staging data.
- **Mapping Tables**: `word_mapping` and `number_mapping` for data standardization.
- **dbt**: Data transformation and integration tool.
- **Airbyte**: ETL tool for data transfer to Snowflake.
- **Snowflake**: Final destination for cleansed and integrated data.
