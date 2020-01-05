# Udacity Capstone Project

This repo contains all the code that were used to build a ETL Pipeline using Apache Spark that extracts data from multiple sources about Immigration to the United States and then transforms the data into a more useful data model in the form of relational tables of a SQL database. The transformed data is first written as `.parquet` files into a bucket in S3 and then loaded into AWS Redshift. Apache Airflow is used to automated the scheduling and monitoring of the jobs. A in depth documentation and purpose of this project can be found in Jupyter Notebook as it made more sense to document everything with the code since its on a Notebook.

### Data model

Adding the Data Model in readme, incase it fails to load in the Jupyter Notebook.

![image](https://github.com/arahman5/udacity-capstone-project/blob/master/model_capstone.PNG)

### Running the scripts

Download the contents of this repo in your Udacity workspace and then run the cells in Jupyter Notebook. You will also need to fill in the `dwh.cfg` file with your AWS details and set up your Apache Airflow environment to run the DAG.

