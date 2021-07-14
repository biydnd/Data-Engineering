# DROP TABLES

songplay_table_drop = "DROP table IF EXISTS songplays "
user_table_drop = "DROP table IF EXISTS users "
song_table_drop = "DROP table IF EXISTS songs "
artist_table_drop = "DROP table IF EXISTS artists "
time_table_drop = "DROP table IF EXISTS time "

# CREATE TABLES

songplay_table_create = ("""
create table if not exists songplays (
    songplay_id serial PRIMARY KEY, 
    start_time bigint, 
    user_id int, 
    level varchar, 
    song_id varchar, 
    artist_id varchar, 
    session_id int, 
    location varchar, 
    user_agent varchar,
    CONSTRAINT user_session UNIQUE (start_time, user_id, level, session_id))
""")

user_table_create = ("""
create table if not exists users(
    user_id int, 
    first_name varchar, 
    last_name varchar, 
    gender varchar, 
    level varchar,
    CONSTRAINT user_identity UNIQUE(user_id, first_name, last_name, gender, level))
""")

song_table_create = ("""
create table if not exists songs(
    song_id varchar PRIMARY KEY, 
    title varchar, 
    artist_id varchar, 
    year int, 
    duration numeric)
""")

artist_table_create = ("""
create table if not exists artists(
    artist_id varchar, 
    name varchar, 
    location varchar, 
    latitude numeric, 
    longitude numeric,
    CONSTRAINT artist_identity UNIQUE(artist_id, name, location)
    )
""")

time_table_create = ("""
create table if not exists time(
    start_time bigint PRIMARY KEY, 
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday int)
""")

# DROP AND CREATE TEMP TABLES
drop_temp_time = "drop table if exists temp_time"
create_temp_time = "create TEMPORARY table temp_time (start_time bigint, hour int, day int, week int,month int, year int, week_day int) ON COMMIT DROP"

drop_temp_users = "drop table if exists temp_users"
create_temp_users = "create TEMPORARY table temp_users (user_id bigint, first_name varchar, last_name varchar, gender varchar,level varchar) ON COMMIT DROP"

drop_temp_songplays = "drop table if exists temp_time"
create_temp_songplays = """create TEMPORARY table temp_songplays (
    start_time bigint,
    user_id int, 
    level varchar, 
    song_id varchar, 
    artist_id varchar, 
    session_id int, 
    location varchar, 
    user_agent varchar) ON COMMIT DROP

"""

# INSERT RECORDS

songplay_table_insert = ("""
insert into songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) values (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT on CONSTRAINT user_session  
DO NOTHING

""")
songplay_table_insert_from_temp =("""
insert into songplays (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent) select * from temp_songplays ON CONFLICT on CONSTRAINT user_session DO NOTHING
""")

user_table_insert = ("""
insert into users(user_id, first_name, last_name, gender, level) values (%s, %s, %s, %s, %s) ON CONFLICT on CONSTRAINT user_identity DO NOTHING
""")
user_table_insert_from_temp = "insert into users select * from  temp_users ON CONFLICT on CONSTRAINT user_identity DO NOTHING"

song_table_insert = ("""
insert into songs(song_id, title, artist_id, year, duration) values (%s, %s, %s, %s, %s) ON CONFLICT (song_id) DO NOTHING
""")

artist_table_insert = ("""
insert into artists(artist_id, name, location, latitude, longitude) values (%s, %s, %s, %s, %s) ON CONFLICT on CONSTRAINT artist_identity  DO NOTHING
""")


time_table_insert = ("""
insert into time(start_time, hour, day, week, month, year, weekday) values (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (start_time) DO NOTHING
""")
time_table_insert_from_temp = "insert into time select * from temp_time ON CONFLICT (start_time) DO NOTHING"

# FIND SONGS

song_select = ("""
select song_id, s.artist_id from songs s join artists a on s.artist_id=a.artist_id where title=%s and name=%s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]