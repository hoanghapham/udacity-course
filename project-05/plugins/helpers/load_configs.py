from abc import ABC, abstractmethod

# Group queries to delete, create and insert tables into LoadConfig objects

insert_songplays_table = ("""
    INSERT INTO public.songplays (
        SELECT
            md5(songs.song_id || events.start_time) playid,
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
        WHERE songs.song_id IS NOT NULL
    )
""")

insert_users_table = ("""
    INSERT INTO public.users (
        SELECT userid, firstname, lastname, gender, level
        FROM (
            SELECT
                userid, firstname, lastname, gender, level,
                ROW_NUMBER() OVER (PARTITION BY userid ORDER BY ts DESC) AS idx
            FROM staging_events
            WHERE page='NextSong'
                AND userid IS NOT NULL
        ) WHERE idx = 1
    )
""")

insert_songs_table = ("""
    INSERT INTO public.songs (
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    )
""")

insert_artists_table = ("""
    INSERT INTO public.artists (
        SELECT  artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY artist_id) AS idx 
            FROM staging_songs
        ) WHERE idx = 1
    )
""")

insert_time_table = ("""
    INSERT INTO public.time (
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
                extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    )
""")

copy_s3_to_table = """
    COPY public.{table}
    FROM '{s3_path}'
    IAM_ROLE '{iam_role}'
    FORMAT AS JSON 'auto'
"""

create_artists_table = """
CREATE TABLE IF NOT EXISTS public.artists (
    artistid varchar(256) NOT NULL,
    name varchar(256),
    location varchar(256),
    lattitude numeric(18,0),
    longitude numeric(18,0)
);
"""

drop_artists_table = "drop table if exists public.artists;"

create_songplays_table = """
CREATE TABLE IF NOT EXISTS public.songplays (
    playid varchar(32) NOT NULL,
    start_time timestamp NOT NULL,
    userid int4,
    "level" varchar(256),
    songid varchar(256),
    artistid varchar(256),
    sessionid int4,
    location varchar(256),
    user_agent varchar(256),
    CONSTRAINT songplays_pkey PRIMARY KEY (playid)
);
"""

drop_songplays_table = "drop table if exists public.songplays;"

create_songs_table = """
CREATE TABLE IF NOT EXISTS public.songs (
    songid varchar(256) NOT NULL,
    title varchar(256),
    artistid varchar(256),
    "year" int4,
    duration numeric(18,0),
    CONSTRAINT songs_pkey PRIMARY KEY (songid)
);
"""

drop_songs_table = "drop table if exists public.songs;"

create_time_table = """
CREATE TABLE IF NOT EXISTS public."time" (
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

drop_time_table = "drop table if exists public.time;"

create_users_table = """
CREATE TABLE IF NOT EXISTS public.users (
    userid int4 NOT NULL,
    first_name varchar(256),
    last_name varchar(256),
    gender varchar(256),
    "level" varchar(256),
    CONSTRAINT users_pkey PRIMARY KEY (userid)
);
"""

drop_users_table = "drop table if exists public.users;"

create_staging_events_table = """
CREATE TABLE IF NOT EXISTS public.staging_events (
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

drop_staging_events_table = "drop table if exists public.staging_events;"

create_staging_songs_table = """
CREATE TABLE IF NOT EXISTS public.staging_songs (
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

drop_staging_songs_table = "drop table if exists public.staging_songs;"


copy_staging_events_table = """
    COPY public.staging_events
    FROM 's3://udacity-dend/log_data'
    IAM_ROLE '{iam_role}'
    FORMAT AS JSON 's3://udacity-dend/log_json_path.json'
"""

copy_staging_songs_table = """
    COPY public.staging_songs
    FROM 's3://udacity-dend/song_data'
    IAM_ROLE '{iam_role}'
    FORMAT AS JSON 'auto'
"""

class LoadConfig(ABC):

    @property
    @abstractmethod
    def table_name(self):
        pass
    
    @property
    @abstractmethod
    def drop_table(self):
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
    """LoadConfig for staging_events table.
    """
    table_name = 'staging_events'
    drop_table = drop_staging_events_table
    create_table = create_staging_events_table
    insert_table = copy_staging_events_table

class StageSongsTable(LoadConfig):
    """LoadConfig for staging_songs table.
    """
    table_name = 'staging_songs'
    drop_table = drop_staging_songs_table
    create_table = create_staging_songs_table
    insert_table = copy_staging_songs_table

class LoadUsersDimTable(LoadConfig):
    """LoadConfig for users table.
    """
    table_name = 'users'
    drop_table = drop_users_table
    create_table = create_users_table
    insert_table = insert_users_table

class LoadSongsDimTable(LoadConfig):
    """LoadConfig for songs table.
    """
    table_name = 'songs'
    drop_table = drop_songs_table
    create_table = create_songs_table
    insert_table = insert_songs_table

class LoadArtistsDimTable(LoadConfig):
    """LoadConfig for artists table.
    """
    table_name = 'artists'
    drop_table = drop_artists_table
    create_table = create_artists_table
    insert_table = insert_artists_table

class LoadTimeDimTable(LoadConfig):
    """LoadConfig for time table.
    """
    table_name = 'time'
    drop_table = drop_time_table
    create_table = create_time_table
    insert_table = insert_time_table

class LoadSongplaysFactTable(LoadConfig):
    """LoadConfig for songplays table.
    """
    table_name = 'songplays'
    drop_table = drop_songplays_table
    create_table = create_songplays_table
    insert_table = insert_songplays_table

