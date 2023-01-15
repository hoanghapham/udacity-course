class InsertQueries:
    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)

class StageQueries:
    stage_table = """
        COPY staging.{table}
        FROM '{s3_path}'
        ACCESS_KEY_ID '{access_key}'
        SECRET_ACCESS_KEY '{secret_key}'
        FORMAT AS JSON 'auto'
    """

class CreateSchemaQueries:
    staging_schema_create = "CREATE SCHEMA IF NOT EXISTS staging" 
    analytics_schema_create = "CREATE SCHEMA IF NOT EXISTS analytics" 
class CreateTableQueries:

    # CREATE TABLES

    staging_events_table_create= ("""
    CREATE TABLE staging.log (
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