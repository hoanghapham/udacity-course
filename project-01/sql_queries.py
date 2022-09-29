# DROP TABLES

songplay_table_drop = ""
user_table_drop = ""
song_table_drop = ""
artist_table_drop = ""
time_table_drop = ""

# CREATE TABLES

songplay_table_create = ("""
    create table if not exists songplays (
        songplay_id integer primary key autoincrement,
        start_time timestamp,
        user_id integer,
        level string,
        song_id string,
        artist_id string,
        session_id integer,
        location string,
        user_agent string
    )
""")

user_table_create = ("""
    create table if not exists users (
        user_id integer primary key,
        first_name string,
        last_name string,
        gender string,
        level string
    )
""")

song_table_create = ("""
    create table if not exists songs (
        song_id string primary key,
        title string,
        artist_id string,
        year integer,
        duration integer
    )
""")

artist_table_create = ("""
    create table if not exists artists (
        artist_id string primary key,
        name string,
        location string,
        latitude float,
        longitude float,
    )
""")

time_table_create = ("""
    create table if not exists time (
        start_time timestamp,
        hour integer,
        day integer,
        week integer,
        month integer,
        year integer,
        weekday string
    )
""")

# INSERT RECORDS

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")


time_table_insert = ("""
""")

# FIND SONGS

song_select = ("""
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]