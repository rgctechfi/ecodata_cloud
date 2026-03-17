The command are MacOS/Linux ready, not for Windows.


generate a picture of the project

# insert map of 2 solutions
- on premise
- local

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

## Local Architecture: Why ?

- No need to pay for cloud services, solution cheap with free tools
- Stay in control of your infrastructure
- Keep data under control and stay sovereign
- Great for a first project in a small/medium enterprise 
- Can be used as a Proof-of-concept before migration to cloud

