# Purposes

This project processes song files and log files from fictitious music store, Skarkify,  stored in file system to extract the song plays information for analysis.  The data are extracted and transformed to load into one fact table, songplays and 4 dimension tables, songs, artists, users and time tables. 

The main projetc executable files are cretae_tables.py and etl.py. The create_tables.py creates sparkify database and all the target tables hence must be run first. After cretae_tables.py is run successfully. Run etl.py to process all files and load the 5 tables.

sql_query.py - contains all create scripts and insertion scripts used in etl.py.

# Schema

### Fcat table
songplays
    songplay_id serial PRIMARY KEY
    start_time bigint 
    user_id int 
    level varcha, 
    song_id varchar 
    artist_id varchar 
    session_id int 
    location varchar 
    user_agent varchar
    CONSTRAINT user_session UNIQUE (start_time, user_id, level, session_id)

### Dimension Tables
users
    user_id int, 
    first_name varchar 
    last_name varchar 
    gender varchar 
    level varchar
    CONSTRAINT user_identity UNIQUE(user_id, first_name, last_name, gender, level)
    
songs
    song_id varchar PRIMARY KEY 
    title varchar 
    artist_id varchar 
    year int 
    duration numeric

artists
    artist_id varchar, 
    name varchar, 
    location varchar, 
    latitude numeric, 
    longitude numeric,
    CONSTRAINT artist_identity UNIQUE(artist_id, name, location)

time
    start_time bigint PRIMARY KEY, 
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday int

The constraints or primary keys in the tables are placed to prevent duplicate information. Upon conflicts during insertion, the program currently does nothing.

# Analytics Examples

Some example analysis with these data are:

### The numbers of free and paid users change through the years
select year, level, sum(distinct(user_id)) as num_users from songplays s join time t on s.start_time=t.start_time group by year, level

### The average days of week the two levels of users listening to the songs
select year, level, round(avg(weekday),2) as avg_weekday from songplays s join time t on s.start_time=t.start_time group by level, year

### The average hours of day the two levels of users listening to the songs
select year, level, round(avg(hour),2) as avg_hour from songplays s join time t on s.start_time=t.start_time group by level, year

### The most popular song by year and level
select year, level, title, max(ct) as ct from (select t.year, level, title, count(*) as ct from songplays p join time t on p.start_time=t.start_time join songs s on p.song_id=s.song_id group by t.year, level, title) a group by year, level, title
