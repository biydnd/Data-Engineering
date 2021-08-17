# Purposes

This project extract and transform song files and log files from fictitious music store, Skarkify, stored in AWS S3 storage by Apoche Spark then store the transformed data into AWS S3 data lake. The program reads the log and song data from S3 then extracts and transoforms the data to load into the five target tables, namely 1 fact table,songplays, and 4 dimension tables, songs, artists, users and time tables. The program then stored these 5 tables in parquet format into an output bucket pre-created for each table in their respective directory. 

Before running etl.py of the project, the S3 output bucket must be created along with the proper aws_access_key_id and aws_secret_access_key for accessing S3 buckets stored in the dl.cfg file.

The main projetc executable file is just etl.py. Once the S3 output bucket is created, enter the output bucket name in etl.py. Run etl.py on terminal or by Spark Clsuster on AWS to load the fact and dimension tables to the S3 output bucket. songs_table files are partitioned by year and then artist. time_table files are partitioned by year and month. songplays_table files are partitioned by year and month.


# Schema

### Fcat table
songplays
    songplay_id         INTEGER,
    start_time          TIMESTAMP,
    user_id             INTEGER,
    level               VARCHAR,
    song_id             VARCHAR,
    artist_id           VARCHAR,
    session_id          INTEGER,
    location            STRING,
    user_agent          STRING,
    year                INTEGER,
    month               INTEGER

### Dimension Tables
users
    user_id             INTEGER,
    first_name          STRING,
    last_name           STRING,
    gender              STRING,
    level               STRING
    
songs
    song_id             STRING,
    title               STRING,
    artist_id           STRING,
    year                INTEGER,
    duration            DOUBLE

artists
    artist_id           STRING,
    name                STRING,
    location            STRING,
    latitude            DOUBLE,
    longitude           DOUBLE

time
    start_time          TIMESTAMP,
    hour                INTEGER,
    day                 INTEGER,
    week                INTEGER,
    month               INTEGER,
    year                INTEGER,
    weekday             INTEGER


