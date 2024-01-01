# Multinational-Retail-Data-Centralisation
You work for a multinational company that sells various goods across the globe.

Currently, their sales data is spread across many different data sources making it not easily accessible or analysable by current members of the team.

In an effort to become more data-driven, your organisation would like to make its sales data accessible from one centralised location.

# Table of Contents
- Descriptions of the project:
- Installation instructions
- Usage instructions
- File structure of the project
- License information
- Experience of going through the project

# Descriptions of the project:
### What is it?
- The aim of the project is to create methods to gather data from many different data sources and make all the data accessible from one centralised location. 
- The centralised location is pre-defined as your local machine where Postgres data base is setup. 
- Data sources including AWS S3, AWS database, PDF, CSV, JSON.
- Data downloaded have to be cleaned before uploading to local database.
- Finally the data analysis to be carried out using uploaded data
- My additional aim is to allow the program to be reusable easily and quickly to support daily or repeate usage. 

### What have I learned?
- Different ways to handle different data sources.
- Without a clear strategy, it can be difficult to determine what actions are needed to clean the data.
- A clear executed data cleaning during the data update process can save time in later analysis stage.

# Installation instructions

- Download the full repo to local machine. Create three files detailed below and place them in the same folder as the python files downloaded.

- Store API details including endpoints, header key and value are to be stored in the file named "store_api_creds.yaml" in the format of

  - >key: "*headers*", value: "*[key: value]*"
  - >key: "*store_number_endpoint*", value: "*url*"
  - >key: "*retrieve_store_endpoint*", value: "*url including the {store_number}*"

- Local Postgres database credential to be stored in the file named "local_db_creds.yaml" in the format of
  - >LDS_DATABASE_TYPE: 'xxxxxx'
  - >LDS_DBAPI: 'xxxxxx'
  - >LDS_HOST: 'xxxxxx'
  - >LDS_USER: 'xxxxxx'
  - >LDS_PASSWORD: 'xxxxxx'
  - >LDS_DATABASE: 'xxxxxx'
  - >LDS_PORT: xxxxxx

- AWS db credentials to be stored in the file named "db_creds.yaml" in the format of
  - >RDS_HOST: 'xxxxxx'
  - >RDS_PASSWORD: 'xxxxxx'
  - >RDS_USER: 'xxxxxx'
  - >RDS_DATABASE: 'xxxxxx'
  - >RDS_PORT: 'xxxxxx'

- Libraries required include: 
  - Pandas via pip install Pandas - *import pandas*
  - PyYAML via pip install PyYAML - *import yaml*
  - tabula-py via pip install tabula-py[jpype] - *import tabula*
  - Python requests - *import requests*
  - Python SDK for AWS via pip install boto3 - *import boto3*

# Usage instructions

Run the python file named "run.py" to run data download, clean and database update.

# File structure of the project

data_cleaning.py - a python class containing methods to clean table types encountered in this project.
data_extraction.py - a python class containing methods to extract data from varies of sources into Pandas DataFrame.
database_utils.py - a python class containing methods to help to execute methods in above classes.
milestone3.sql - 
milestone4.sql - 
run.py - execution file offers different methods to complete stages in the project. This can also be used to quickly update existing tables on local database for daily usage.

# License information

Users are allowed to do whatever they want with the code (including commercial use) as long as they provide attribution and include the original license in any copies or substantial uses of the work.

# Experience of going through the project

- It was quite straightforward on what needs to be done at each stage, but the hard bit happened when it come to the data cleanning at each of the table. With my first time involved in such task, I didn't know how clean the table should be before uploading to database.
- I probably ended up with over processing with too many filtering conditions where I have to removed some of them in order to allow works in later milestones.
- I think unfamiliar of table or their content also contribute issue encountered in above point. In real world, one should make themselves familiar with the business and their data before starting to process the data. 