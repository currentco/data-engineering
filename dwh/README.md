# Project: Data Modeling with Amazon Redshift

Background: A music streaming startup called Sparkify has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This project is made to build an ETL pipeline that extracts the data from S3, stages it in Redshift, and transforms it into a set of dimensional tables. This ables querying the JSON logs and metadata in Amazon Redshift database, which has tables designed to optimize queries on song play analysis.

## Database design

Built with Python & SQL in sql_queries.py and create_tables.py files.
Database is built as a star schema.

**Fact Table:**
- songplays - records in log data associated with song plays i.e. records with page NextSong

**Dimension Tables:**
- users - users in the app
- songs - songs in music database
- artists - artists in music database
- time - timestamps of records in songplays broken down into specific units

## ETL pipeline

Built with Python & SQL in sql_queries.py and etl.py files.
The ETL pipeline transfers song, artist, user and log data from files in two Amazon S3 cloud directories (log_data & song_data) first into the staging tables and then into the fact and dimension tables in Amazon Redshift database.

**Stages:**

1. Starts the connection to the Redshift database and the cursor. 
2. Copies and processes the data within the two separate S3 directories to the staging tables in Redshift.
3. Log data processing from a staging table into the database dimension tables filtered by the NextSong action. Song data processing from a staging table into the database dimension tables. Combining log and song data staging tables into the songplays fact table.
4. Extracting the timestamp data and inserting the data into a time dimension table with various time variables.
5. Closes the connection to the database.


## Project Repository files

- create_tables.py - Drops and creates the tables. Run this file to reset the tables before each time running the ETL scripts.
- etl.py - Reads and processes files from song_data and log_data and loads them into the tables.
- sql_queries.py - Contains all the SQL queries, and is imported into the two files above.
- README.md - Provides discussion on this project.

## How To Run the Project

### Create Tables

Run create_tables.py to create the database and tables.

### Run ETL Pipeline

Run etl.py to process the dataset.