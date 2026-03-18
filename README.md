The command are MacOS/Linux ready, not for Windows.
The project is a foundation for a big project where the goal is to aggregate economical values

generate a picture of the project

# insert map of solution
- cloud

- parquet is not mandatory for this size of data but I think scalable, and later the project could be bigger.

Solution requierements :
- Selecting a dataset :
- Creating a pipeline for processing this dataset and putting it to a datalake
- Creating a pipeline for moving the data from the lake to a data warehouse
- Transforming the data in the data warehouse: prepare it for the dashboard
- Building a dashboard to visualize the data

# Data pipeline
Batch: run things periodically (e.g. hourly/daily)

# Data stack architecture choice:

## The stack

Container: Docker
Data lake:
Workflow orchestration: Airflow,
OLAP Database:
Batch processing:

Langages : Python, SQL, Pyspark

## Cloud Architecture: Why ?



# Evaluation metrics:

Problem description
Problem is well described and it's clear what the problem the project solves

Cloud
The project is developed in the cloud and IaC tools are used

Batch / Workflow orchestration:
End-to-end pipeline: multiple steps in the DAG, uploading data to data lake

Data warehouse
Tables are partitioned and clustered in a way that makes sense for the upstream queries (with explanation)

Transformations (dbt, spark, etc)
Tranformations are defined with dbt, Spark or similar technologies

Dashboard 
A dashboard with 2 tiles

Reproducibility
Instructions are clear, it's easy to run the code, and the code works

# Read the CONTEXT.md to understand important things