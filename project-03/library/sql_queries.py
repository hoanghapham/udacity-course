import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging.events CASCADE"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging.songs CASCADE"
songplays_table_drop = "DROP TABLE IF EXISTS analytics.songplays CASCADE"
users_table_drop = "DROP TABLE IF EXISTS analytics.users CASCADE"
songs_table_drop = "DROP TABLE IF EXISTS analytics.songs CASCADE"  
artists_table_drop = "DROP TABLE IF EXISTS analytics.artists CASCADE"
time_table_drop = "DROP TABLE IF EXISTS analytics.times CASCADE"


# CHECK TABLES
staging_events_table_check = "SELECT count(*) FROM staging.events"
staging_songs_table_check = "SELECT count(*) FROM staging.songs"
songplays_table_check = "SELECT count(*) FROM analytics.songplays"
users_table_check = "SELECT count(*) FROM analytics.users"
songs_table_check = "SELECT count(*) FROM analytics.songs"
artists_table_check = "SELECT count(*) FROM analytics.artists"
time_table_check = "SELECT count(*) FROM analytics.times"


# DROP SCHEMAS
staging_schema_drop = "DROP SCHEMA IF EXISTS staging CASCADE"
analytics_schema_drop = "DROP SCHEMA IF EXISTS analytics CASCADE"


# CREATE SCHEMAS
staging_schema_create = "CREATE SCHEMA IF NOT EXISTS staging" 
analytics_schema_create = "CREATE SCHEMA IF NOT EXISTS analytics" 


# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging.events (
    artist              VARCHAR,
    auth                VARCHAR(20),
    firstName           VARCHAR,
    gender              VARCHAR(2),
    itemInSession       INTEGER,
    lastName            VARCHAR,
    length              NUMERIC,
    level               VARCHAR(10),
    location            VARCHAR,
    method              VARCHAR(10),
    page                VARCHAR,
    registration        NUMERIC,
    sessionId           INTEGER,
    song                VARCHAR,
    status              INTEGER,
    ts                  BIGINT,
    userAgent           VARCHAR(max),
    userId              INTEGER
)
"""
)

staging_songs_table_create = ("""
CREATE TABLE staging.songs (
    artist_id           VARCHAR,
    artist_latitude     NUMERIC,
    artist_location     VARCHAR,
    artist_longitude    NUMERIC,
    artist_name         VARCHAR,
    duration            NUMERIC,
    num_songs           INTEGER,
    song_id             VARCHAR PRIMARY KEY,
    title               VARCHAR,
    year                INTEGER
)
""")

songplays_table_create = ("""
CREATE TABLE analytics.songplays (
    songplay_id         INTEGER IDENTITY(0, 1), 
    ts                  BIGINT,
    start_time          TIMESTAMP, 
    user_id             INTEGER, 
    level               VARCHAR(10), 
    song_id             VARCHAR, 
    artist_id           VARCHAR, 
    session_id          INTEGER, 
    location            VARCHAR, 
    users_agent         VARCHAR(max),

    FOREIGN KEY (ts) REFERENCES analytics.times (ts),
    FOREIGN KEY (user_id) REFERENCES analytics.users (user_id),
    FOREIGN KEY (artist_id) REFERENCES analytics.artists (artist_id),
    FOREIGN KEY (song_id) REFERENCES analytics.songs (song_id)
)

DISTKEY(ts)
SORTKEY(ts)
""")

users_table_create = ("""
CREATE TABLE analytics.users (
    user_id             INTEGER UNIQUE PRIMARY KEY, 
    first_name          VARCHAR, 
    last_name           VARCHAR, 
    gender              VARCHAR(2), 
    level               VARCHAR(10)
)
DISTSTYLE ALL
""")

songs_table_create = ("""
CREATE TABLE analytics.songs (
    song_id             VARCHAR UNIQUE PRIMARY KEY, 
    title               VARCHAR, 
    artist_id           VARCHAR, 
    year                INTEGER, 
    duration            NUMERIC,
    
    FOREIGN KEY (artist_id) REFERENCES analytics.artists (artist_id)
)
DISTSTYLE ALL
""")

artists_table_create = ("""
CREATE TABLE analytics.artists (
    artist_id           VARCHAR UNIQUE PRIMARY KEY, 
    name                VARCHAR, 
    location            VARCHAR, 
    lattitude           NUMERIC, 
    longitude           NUMERIC
)
DISTSTYLE ALL
""")

time_table_create = ("""
CREATE TABLE analytics.times (
    ts                  BIGINT UNIQUE PRIMARY KEY, 
    start_time          TIMESTAMP,
    date                DATE,    
    hour                INTEGER, 
    day                 INTEGER, 
    week                INTEGER, 
    month               INTEGER, 
    year                INTEGER, 
    weekday             INTEGER
)
DISTSTYLE ALL
""")


# STAGING TABLES
staging_events_copy = ("""
COPY staging.events
FROM '{source_data}'
CREDENTIALS 'aws_iam_role={aws_iam_role}'
FORMAT AS JSON '{log_jsonpath}'
""").format(
    source_data=config['S3']['LOG_DATA']
    , aws_iam_role=config['IAM_ROLE']['ARN']
    , log_jsonpath=config['S3']['LOG_JSONPATH']
)

staging_songs_copy = ("""
COPY staging.songs
FROM '{source_data}'
CREDENTIALS 'aws_iam_role={aws_iam_role}'
FORMAT AS JSON 'auto'
""").format(
    source_data=config['S3']['SONG_DATA']
    , aws_iam_role=config['IAM_ROLE']['ARN']
)


# FINAL ANALYTICS TABLES

songplays_table_insert = ("""
INSERT INTO analytics.songplays (
    ts, 
    start_time, 
    user_id, 
    level, 
    song_id, 
    artist_id, 
    session_id, 
    location, 
    users_agent
)
(
    SELECT
        events.ts,
        (timestamp 'epoch' + events.ts / 1000 * interval '1 second') AS start_time,
        events.userId AS user_id,
        events.level,
        songs.song_id,
        songs.artist_id,
        events.sessionId AS session_id,
        events.location,
        events.userAgent AS user_agent
    FROM staging.events
    LEFT JOIN staging.songs on events.song = songs.title and events.artist = songs.artist_name
)
""")

users_table_insert = ("""
INSERT INTO analytics.users 
(
    WITH latest_records AS (
        SELECT
            userId,
            max(ts) AS latest_ts
        FROM staging.events
        group by 1
    )
    SELECT 
        events.userId AS user_id,
        firstName AS first_name,
        lastName AS last_name,
        gender,
        level
    FROM staging.events
    INNER JOIN latest_records 
        on events.userId = latest_records.userId 
        and events.ts = latest_records.latest_ts
)
""")

song_table_insert = ("""
INSERT INTO analytics.songs 
(
    SELECT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging.songs
)
""")

artist_table_insert = ("""
INSERT INTO analytics.artists
(
    SELECT distinct
        artist_id
        , artist_name AS name
        , artist_location AS location
        , artist_latitude AS lattitude
        , artist_longitude AS longitude
    FROM staging.songs
)
""")

time_table_insert = ("""
INSERT INTO analytics.times
(
    WITH conversion AS (
        SELECT distinct 
            ts,
            (timestamp 'epoch' + ts / 1000 * interval '1 second') AS start_time
        FROM staging.events
    )

    SELECT
        ts,
        start_time,
        date(start_time) AS date,
        EXTRACT(hour FROM start_time) AS hour,
        EXTRACT(day FROM start_time) AS day,
        EXTRACT(week FROM start_time) AS week,
        EXTRACT(month FROM start_time) AS month,
        EXTRACT(year FROM start_time) AS year,
        EXTRACT(dayofweek FROM start_time) AS weekday
    FROM conversion
)
""")


# QUERY LISTS
# Order of queries is important. 
# Dimension tables like users, artists, songs, times... are created before the fact tables (songplays)
create_table_queries = [
    staging_events_table_create, 
    staging_songs_table_create, 
    users_table_create, 
    artists_table_create, 
    songs_table_create, 
    time_table_create,
    songplays_table_create 
]

drop_table_queries = [
    staging_events_table_drop, 
    staging_songs_table_drop, 
    songplays_table_drop, 
    users_table_drop, 
    songs_table_drop, 
    artists_table_drop, 
    time_table_drop
]

copy_table_queries = [
    staging_events_copy, 
    staging_songs_copy
]

insert_table_queries = [
    songplays_table_insert, 
    users_table_insert, 
    song_table_insert, 
    artist_table_insert, 
    time_table_insert
]

create_schema_queries = [
    staging_schema_create,
    analytics_schema_create
]

drop_schema_queries = [
    staging_schema_drop, 
    analytics_schema_drop
]

check_table_queries = [
    staging_events_table_check, 
    staging_songs_table_check, 
    songplays_table_check, 
    users_table_check, 
    songs_table_check, 
    artists_table_check, 
    time_table_check
]