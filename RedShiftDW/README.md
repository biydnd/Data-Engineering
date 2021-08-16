# Purposes

This project processes song files and log files from fictitious music store, Skarkify,  stored in AWS S3 storage and ingests the extracted the song plays information to an AWS Redshift data warehouse for extracting insights on user behaviors as recorded in the log. It's a two-steps ingestion, the program frist copies the log and song data from S3 to two staging tables on Redshift data warehouse respectively. Then extract and transoform the data on the staging tables to load into the five target tables, namely 1 fact table,songplays, and 4 dimension tables, songs, artists, users and time tables. 

Before the running the executables of the project, the AWS redshift cluster must be created with the proper credentials and authorized IAM role.  The cluster in this project was created seperately using Python JDK and the Infrastructure-as-code.

The main projetc executable files are cretae_tables.py and etl.py. One the Redshift cluster is created, the create_tables.py must be run first to create the two staging tables, 1 fact table and 4 dimension tables. After cretae_tables.py is run successfully. Run etl.py to load staging tables and the fact and dimension tables.

sql_query.py - contains all drop scripts, create scripts and insertion scripts used in etl.py.

# Schema

### Staging tables
staging_events
    artist              VARCHAR
    auth                VARCHAR
    firstname           VARCHAR
    gender              VARCHAR
    iteminsession       INTEGER
    lastname            VARCHAR
    length              FLOAT
    level               VARCHAR
    location            VARCHAR
    method              VARCHAR
    page                VARCHAR
    registration        BIGINT
    sessionid           INTEGER
    song                VARCHAR
    status              VARCHAR
    ts                  TIMESTAMP
    useragent           VARCHAR
    userid              INTEGER
    
staging_songs
    num_songs           INTEGER,
    artist_id           VARCHAR,
    artist_latitude     NUMERIC,
    artist_longitude    NUMERIC,
    artist_location     VARCHAR,
    artist_name         VARCHAR,
    song_id             VARCHAR,
    title               VARCHAR,
    duration            FLOAT,
    year                INTEGER
    

### Fcat table
songplays
    songplay_id         INTEGER     IDENTITY(0,1) PRIMARY KEY,
    start_time          TIMESTAMP   NOT NULL,
    user_id             INTEGER     NOT NULL,
    level               VARCHAR,
    song_id             VARCHAR,
    artist_id           VARCHAR,
    session_id          INTEGER     NOT NULL,
    location            VARCHAR,
    user_agent          VARCHAR

### Dimension Tables
users
    user_id             INTEGER     PRIMARY KEY,
    first_name          VARCHAR     NOT NULL,
    last_name           VARCHAR     NOT NULL,
    gender              VARCHAR,
    level               VARCHAR     NOT NULL
    
songs
    song_id             VARCHAR     PRIMARY KEY,
    title               VARCHAR     NOT NULL,
    artist_id           VARCHAR     NOT NULL,
    year                INTEGER,
    duration            NUMERIC

artists
    artist_id           VARCHAR     PRIMARY KEY,
    name                VARCHAR     NOT NULL,
    location            VARCHAR,
    latitude            NUMERIC,
    longitude           NUMERIC

time
    start_time          TIMESTAMP   PRIMARY KEY,
    hour                INTEGER     NOT NULL,
    day                 INTEGER     NOT NULL,
    week                INTEGER     NOT NULL,
    month               INTEGER     NOT NULL,
    year                INTEGER     NOT NULL,
    weekday             INTEGER     NOT NULL

There are no primary keys on the two staging tables as the purpose of the staging table is merely staging the data as they are extracted from the files in S3. The primary keys in the tables are placed to prevent duplicate information. Depending on the availability of data, only some columns are required to be NOT NULL. The daya type of each column was determined based on the data in the files.

# Analytics Examples

Some example analysis with these data are:

### The numbers of free and paid users change through the years
    select 
        year, 
        level, 
        sum(distinct(user_id)) as num_users 
    from 
        songplay s 
    join 
        time t 
    on 
        s.start_time=t.start_time 
    group by 
    year, level

### The average days of week the two levels of users listening to the songs
    select 
        year, level, round(avg(weekday),2) as avg_weekday 
    from 
        songplay s 
    join 
        time t 
    on 
        s.start_time=t.start_time 
    group by 
        level, year

### The average hours of day the two levels of users listening to the songs
    select 
        year, 
        level, 
        round(avg(hour),2) as avg_hour 
    from 
        songplay s
    join 
        time t 
    on 
        s.start_time=t.start_time 
    group by 
        level, year

### The most popular songs by year and level
    select 
        t.year, 
        level, 
        title, 
        count(*) as ct 
    from 
        songplay p 
    join 
        time t 
    on 
        p.start_time=t.start_time 
    join 
        song s 
    on 
        p.song_id=s.song_id 
    group by 
        t.year, level, title
    having ct > 1
    order by
        ct desc