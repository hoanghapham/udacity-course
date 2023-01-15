from abc import ABC, abstractmethod

insert_songplays_table = ("""
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

insert_users_table = ("""
    SELECT distinct userid, firstname, lastname, gender, level
    FROM staging_events
    WHERE page='NextSong'
""")

insert_songs_table = ("""
    SELECT distinct song_id, title, artist_id, year, duration
    FROM staging_songs
""")

insert_artists_table = ("""
    SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs
""")

insert_time_table = ("""
    SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
            extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
    FROM songplays
""")

copy_s3_to_table = """
    COPY public.{table}
    FROM '{s3_path}'
    ACCESS_KEY_ID '{access_key}'
    SECRET_ACCESS_KEY '{secret_key}'
    FORMAT AS JSON 'auto'
"""

create_artists_table = """
CREATE TABLE public.artists (
    artistid varchar(256) NOT NULL,
    name varchar(256),
    location varchar(256),
    lattitude numeric(18,0),
    longitude numeric(18,0)
);
"""

create_songplays_table = """
CREATE TABLE public.songplays (
    playid varchar(32) NOT NULL,
    start_time timestamp NOT NULL,
    userid int4 NOT NULL,
    "level" varchar(256),
    songid varchar(256),
    artistid varchar(256),
    sessionid int4,
    location varchar(256),
    user_agent varchar(256),
    CONSTRAINT songplays_pkey PRIMARY KEY (playid)
);
"""

create_songs_table = """
CREATE TABLE public.songs (
    songid varchar(256) NOT NULL,
    title varchar(256),
    artistid varchar(256),
    "year" int4,
    duration numeric(18,0),
    CONSTRAINT songs_pkey PRIMARY KEY (songid)
);
"""

create_staging_events_table = """
CREATE TABLE public.staging_events (
    artist varchar(256),
    auth varchar(256),
    firstname varchar(256),
    gender varchar(256),
    iteminsession int4,
    lastname varchar(256),
    length numeric(18,0),
    "level" varchar(256),
    location varchar(256),
    "method" varchar(256),
    page varchar(256),
    registration numeric(18,0),
    sessionid int4,
    song varchar(256),
    status int4,
    ts int8,
    useragent varchar(256),
    userid int4
);
"""

create_songs_table = """
CREATE TABLE public.staging_songs (
    num_songs int4,
    artist_id varchar(256),
    artist_name varchar(256),
    artist_latitude numeric(18,0),
    artist_longitude numeric(18,0),
    artist_location varchar(256),
    song_id varchar(256),
    title varchar(256),
    duration numeric(18,0),
    "year" int4
);
"""

create_time_table = """
CREATE TABLE public."time" (
    start_time timestamp NOT NULL,
    "hour" int4,
    "day" int4,
    week int4,
    "month" varchar(256),
    "year" int4,
    weekday varchar(256),
    CONSTRAINT time_pkey PRIMARY KEY (start_time)
) ;
"""

create_users_table = """
CREATE TABLE public.users (
    userid int4 NOT NULL,
    first_name varchar(256),
    last_name varchar(256),
    gender varchar(256),
    "level" varchar(256),
    CONSTRAINT users_pkey PRIMARY KEY (userid)
);
"""

class DataQualityQueries:
    check_unique_sql = """
        sellect
            {column},
            count(*) as cnt
        from {table}
        group by 1
        having cnt > 1
    """

    check_not_null_sql = """
        select
            count(case when {column} is null then 1 else null end) as cnt
        from {table}
        having cnt > 0
    """

    check_has_data = """
        select 
            count(*) as cnt
        from {table}
        having cnt > 0
    """

copy_staging_events_table = """
    COPY public.staging_events
    FROM 's3://udacity-dend/log_data'
    ACCESS_KEY_ID '{access_key}'
    SECRET_ACCESS_KEY '{secret_key}'
    FORMAT AS JSON 'auto'
"""

copy_staging_songs_table = """
    COPY public.staging_songs
    FROM 's3://udacity-dend/song_data'
    ACCESS_KEY_ID '{access_key}'
    SECRET_ACCESS_KEY '{secret_key}'
    FORMAT AS JSON 'auto'
"""

class LoadConfig(ABC):

    @property
    @abstractmethod
    def table_name(self):
        pass

    @property
    @abstractmethod
    def create_table(self):
        pass

    @property
    @abstractmethod
    def insert_table(self):
        pass


class StageEventsTable(LoadConfig):
    table_name = 'staging_events'
    create_table = create_staging_events_table
    insert_table = copy_staging_events_table

class StageSongsTable(LoadConfig):
    table_name = 'staging_songs'
    create_table = create_songs_table
    insert_table = copy_staging_songs_table

class LoadUsersDimTable(LoadConfig):
    table_name = 'users'
    create_table = create_users_table
    insert_table = insert_users_table

class LoadSongsDimTable(LoadConfig):
    table_name = 'songs'
    create_table = create_songs_table
    insert_table = insert_songs_table

class LoadArtistsDimTable(LoadConfig):
    table_name = 'artists'
    create_table = create_artists_table
    insert_table = insert_artists_table

class LoadTimeDimTable(LoadConfig):
    table_name = 'time'
    create_table = create_time_table
    insert_table = insert_time_table