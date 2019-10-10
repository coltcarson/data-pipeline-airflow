# Data Pipelines with Airflow

For this project, we created a music streaming ETL pipeline using Python, Airflow, and Redshift. We began by using Python to create an Airflow Pipeline to extract user listening and song metadata JSON files from S3 buckets so that they could be staged and transformed within AWS Redshift.

--------------------------------------------

# Schema
![Airflow Data Pipeline](https://github.com/coltcarson/data-pipeline-airflow/blob/master/example-dag.png)

--------------------------------------------

# Files
1) udac_example_dag.py - Script handles the construction of Airflow parameter needs to run the data pipeline.
2) data_quality.py - Data quality check used to validate records input into Redshift.
3) load_dimension.py - Loads dimension data into Redshift.
4) load_fact.py - Loads Fact data into Redshift.
5) stage_redshift.py - Copies data to staging tables in Redshift from JSON


--------------------------------------------

# Instructions
1) Type run /opt/airflow/start.sh to run Airflow from terminal.
2) Input AWS and Redshift credentials into Airflow via UI.
3) Run dag and monitor results.
