import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
log_data = config.get("S3","LOG_DATA")
song_data = config.get("S3","SONG_DATA")
log_jsonpath = config.get("S3","LOG_JSONPATH")
role_arn = config.get("IAM_ROLE_ARN","ROLE_ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"


# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events (
    artist           VARCHAR,
    auth             VARCHAR,
    firstname        VARCHAR,
    gender           VARCHAR,
    iteminsession    INTEGER,
    lastname         VARCHAR,
    length           FLOAT,
    level            VARCHAR,
    location         VARCHAR,
    method           VARCHAR,
    page             VARCHAR,
    registration     BIGINT,
    sessionid        INTEGER,
    song             VARCHAR,
    status           VARCHAR,
    ts               TIMESTAMP,
    useragent        VARCHAR,
    userid           INTEGER
)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
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
)
""")
                        
songplay_table_create  = ("""
CREATE TABLE songplay (
    songplay_id     INTEGER    IDENTITY(0,1) PRIMARY KEY,
    start_time      TIMESTAMP  NOT NULL,
    user_id         INTEGER    NOT NULL,
    level           VARCHAR,
    song_id         VARCHAR,
    artist_id       VARCHAR,
    session_id      INTEGER    NOT NULL,
    location        VARCHAR,
    user_agent      VARCHAR
)
""")

user_table_create = ("""
CREATE TABLE users (
    user_id         INTEGER     PRIMARY KEY,
    first_name      VARCHAR     NOT NULL,
    last_name       VARCHAR     NOT NULL,
    gender          VARCHAR,
    level           VARCHAR     NOT NULL
)
""")

song_table_create = ("""
CREATE TABLE song (
    song_id        VARCHAR     PRIMARY KEY,
    title          VARCHAR     NOT NULL,
    artist_id      VARCHAR     NOT NULL,
    year           INTEGER,
    duration       NUMERIC
)
""")

artist_table_create = ("""
CREATE TABLE artist (
    artist_id       VARCHAR    PRIMARY KEY,
    name            VARCHAR    NOT NULL,
    location        VARCHAR,
    latitude        NUMERIC,
    longitude       NUMERIC
)
""")

time_table_create = ("""
CREATE TABLE time 
(
    start_time      TIMESTAMP PRIMARY KEY,
    hour            INTEGER NOT NULL,
    day             INTEGER NOT NULL,
    week            INTEGER NOT NULL,
    month           INTEGER NOT NULL,
    year            INTEGER NOT NULL,
    weekday         INTEGER NOT NULL
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
credentials 'aws_iam_role={}'
EMPTYASNULL
BLANKSASNULL
FORMAT AS json {}
compupdate off region 'us-west-2'
timeformat as 'epochmillisecs';
""").format(log_data,role_arn,log_jsonpath)

staging_songs_copy = ("""
COPY staging_songs FROM {}
credentials 'aws_iam_role={}'
json 'auto'
compupdate off region 'us-west-2';
""").format(song_data,role_arn)

# FINAL TABLES

songplay_table_insert2 = ("""
    insert into songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    select
        TIMESTAMP 'epoch' + ts/1000 * interval '1 second' as start_time,
        userid,
        level,
        s.song_id,
        a.artist_id,
        sessionid,
        e.location,
        useragent
    from
        staging_events e
    join 
        staging_songs s
    on
        e.song=s.title
        and
        e.artist=s.artist_name
        and
        e.length=s.duration
    where 
        e.page='NextSong'
""")

songplay_table_insert = ("""
    insert into songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    select
        ts,
        userid,
        level,
        s.song_id,
        s.artist_id,
        sessionid,
        e.location,
        useragent
    from
        staging_events e
    join 
        staging_songs s
    on
        e.song=s.title
        and
        e.artist=s.artist_name
        and
        e.length=s.duration
    where 
        e.page='NextSong'
""")

user_table_insert = ("""
    insert into users (user_id, first_name, last_name, gender, level)
    select
        distinct(userid),
        firstname,
        lastname,
        gender,
        level
    from
        staging_events
    where
        page = 'NextSong'
    
""")

song_table_insert = ("""
    insert into song (song_id, title, artist_id, year, duration)
    select 
        distinct(song_id),
        title,
        artist_id,
        year,
        duration
    from 
        staging_songs
""")

artist_table_insert = ("""
    insert into artist (artist_id, name, location, latitude, longitude)
    select
        distinct(artist_id),
        artist_name as name,
        artist_location,
        artist_latitude,
        artist_longitude
    from
        staging_songs
        
        
""")

time_table_insert = ("""
    insert into time (start_time, hour, day, week, month, year, weekday)
    select
        distinct(ts), 
        extract(hour from ts),
        extract(day from ts),
        extract(week from ts),
        extract(month from ts),
        extract(year from ts),
        extract(dow from ts)
    from
        staging_events
    where
        page = 'NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
