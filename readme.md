
```markdown

## Task 1: Data Ingestion and ETL

### Setup Instructions

**1. Install Requirements**

Install the required Python packages:

```bash
pip install -r requirements.txt
```

### 0. Generate Data

Use the provided module to generate data:

```bash
python3 genrate_data.py --num_rows 1000 --file_path data.csv
```

### 1. Data Ingestion

Ingest the generated data and store it in `rep_db`:

```bash
python3 ingest.py --genrated_file_path ../gen_data/data.csv --db_path rep_db.db --table_name rep_interaction
```

### 2. Data Cleaning

Clean the ingested data and store it in `raw_db`:

```bash
python3 clean.py --ingested_db_path ../ingest/rep_db.db --ingested_table_name rep_interaction --cleaned_db_path raw_db.db --cleaned_table_name raw_interaction
```

### 3. Data Transformation

Transform the cleaned data and add data retrieval scripts. The `pub_db` will contain the following tables:

- `pub_interaction`: Transformed and cleaned table
- `interactions_count`: Table with `interactions_count` column added
- `interactions_per_day`: Number of interactions per day
- `top_5_users`: Top 5 users by number of interactions

Run the transformation script:

```bash
python3 transform.py --clean_db_path ../clean/raw_db.db --clean_table_name raw_interaction --transformed_db_path pub_db.db --transformed_table_name pub_interaction
```

### 4. Data Loading

Data will be loaded into the following databases after each step:

- **Ingestion:** `rep_db` (Replication Database)
- **Cleaning:** `raw_db` (Raw Zone)
- **Transformation:** `pub_db` (Publication Zone)

## Task 2: Data Pipeline with Apache Airflow

Created an Airflow DAG to automate these modules as Python operators.

## Task 3: Data Storage and Retrieval

The SQLite database has been created and shared. In `pub_db`, all required tables as per instructions are stored.
```

This version provides a clearer structure and more detailed descriptions of each step.
