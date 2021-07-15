# DROP TABLES

songplay_table_drop = "DROP table IF EXISTS songplays "
user_table_drop = "DROP table IF EXISTS users "
song_table_drop = "DROP table IF EXISTS songs "
artist_table_drop = "DROP table IF EXISTS artists "
time_table_drop = "DROP table IF EXISTS time "

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays 
(
    songplay_id SERIAL PRIMARY KEY, 
    start_time BIGINT NOT NULL, 
    user_id INT NOT NULL, 
    level VARCHAR NOT NULL, 
    song_id VARCHAR, 
    artist_id VARCHAR, 
    session_id INT NOT NULL, 
    location VARCHAR, 
    user_agent VARCHAR,
    CONSTRAINT user_session UNIQUE (start_time, user_id, artist_id, level, song_id,location,user_agent, session_id)
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
    user_id INT PRIMARY KEY, 
    first_name VARCHAR, 
    last_name VARCHAR, 
    gender VARCHAR, 
    level VARCHAR NOT NULL
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
    song_id VARCHAR PRIMARY KEY, 
    title VARCHAR NOT NULL, 
    artist_id VARCHAR NOT NULL, 
    year INT, 
    duration NUMERIC
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
    artist_id VARCHAR PRIMARY KEY, 
    name VARCHAR NOT NULL, 
    location VARCHAR, 
    latitude NUMERIC, 
    longitude NUMERIC
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
    start_time BIGINT PRIMARY KEY, 
    hour INT, 
    day INT, 
    week INT, 
    month INT, 
    year INT, 
    weekday INT
)
""")

# DROP AND CREATE TEMP TABLES
drop_temp_time = "DROP table IF EXISTS temp_time"
create_temp_time = ("""
    CREATE TEMPORARY TABLE temp_time 
    (
        start_time bigint, 
        hour int, 
        day int, 
        week int,
        month int, 
        year int, 
        week_day int
    ) 
    ON COMMIT DROP
""")
drop_temp_users = "DROP table IF EXISTS temp_users"
create_temp_users = ("""
    CREATE TEMPORARY TABLE temp_users 
    (
        user_id bigint, 
        first_name varchar, 
        last_name varchar, 
        gender varchar,
        level varchar
    ) 
    ON COMMIT DROP
""")

drop_temp_songplays = "DROP table IF EXISTS temp_time"
create_temp_songplays = ("""
    CREATE TEMPORARY TABLE temp_songplays 
    (
        start_time bigint,
        user_id int, 
        level varchar, 
        song_id varchar, 
        artist_id varchar, 
        session_id int, 
        location varchar, 
        user_agent varchar
    )
    ON COMMIT DROP
""")

# INSERT RECORDS

songplay_table_insert = ("""
    INSERT INTO songplays
    (
        start_time, 
        user_id, 
        level, 
        song_id, 
        artist_id, 
        session_id, 
        location, 
        user_agent
    ) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s) 
    ON CONFLICT on CONSTRAINT user_session  
    DO NOTHING
""")
songplay_table_insert_from_temp = ("""
    INSERT INTO songplays
    (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    )
        select * from temp_songplays 
    ON CONFLICT on CONSTRAINT user_session 
    DO NOTHING
""")

user_table_insert = ("""
    INSERT INTO users
    (
        user_id, 
        first_name, 
        last_name, 
        gender, 
        level
    ) 
    VALUES (%s, %s, %s, %s, %s) 
    ON CONFLICT (user_id)
    DO UPDATE
        set level=EXCLUDED.level
""")
user_table_insert_from_temp = ("""
    INSERT INTO users 
        SELECT * FROM  temp_users 
    ON CONFLICT (user_id)
    DO UPDATE
        set level=EXCLUDED.level
""")

song_table_insert = ("""
    INSERT INTO songs
    (
        song_id, 
        title, 
        artist_id, 
        year, 
        duration
    ) 
    VALUES (%s, %s, %s, %s, %s) 
    ON CONFLICT (song_id) 
    DO UPDATE
        SET title=EXCLUDED.title
""")

artist_table_insert = ("""
    INSERT INTO artists
    (
        artist_id, 
        name, 
        location, 
        latitude, 
        longitude
    ) 
    VALUES (%s, %s, %s, %s, %s) 
    ON CONFLICT (artist_id)
    DO UPDATE
        SET name=EXCLUDED.name
""")


time_table_insert = ("""
    INSERT INTO time(
        start_time, 
        hour, 
        day, 
        week, 
        month, 
        year, 
        weekday) 
    VALUES (%s, %s, %s, %s, %s, %s, %s) 
    ON CONFLICT (start_time) 
    DO NOTHING
""")
time_table_insert_from_temp = ("""
    INSERT INTO time 
        SELECT * FROM temp_time 
    ON CONFLICT (start_time) 
    DO NOTHING
""")

# FIND SONGS

song_select = ("""
    SELECT 
        song_id, 
        s.artist_id 
    FROM 
        songs s
    JOIN artists a 
        ON s.artist_id=a.artist_id 
    WHERE 
        title=%s and name=%s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]