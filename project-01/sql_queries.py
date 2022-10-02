# DROP TABLES

songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

songplay_table_create = ("""create table if not exists songplays (
        songplay_id serial primary key,
        start_time timestamp,
        user_id integer,
        level text,
        song_id text,
        song_title text,
        artist_id text,
        artist_name text,
        session_id integer,
        location text,
        user_agent text
    )
""")

user_table_create = ("""create table if not exists users (
        user_id integer primary key,
        first_name text,
        last_name text,
        gender text,
        level text
    )
""")

song_table_create = ("""create table if not exists songs (
        song_id text primary key,
        title text,
        artist_id text,
        year integer,
        duration integer
    )
""")

artist_table_create = ("""create table if not exists artists (
        artist_id text primary key,
        name text,
        location text,
        latitude float,
        longitude float
    )
""")

time_table_create = ("""create table if not exists time (
        start_time timestamp,
        hour integer,
        day integer,
        week integer,
        month integer,
        year integer,
        weekday text
    )
""")

# INSERT RECORDS

songplay_table_insert = ("""insert into 
    songplays (
        start_time
        , user_id
        , level
        , song_id
        , song_title
        , artist_id
        , artist_name
        , session_id
        , location
        , user_agent
    )
    values 
    (%s, nullif(%s::text, '')::int, %s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""insert into users (user_id, first_name, last_name, gender, level) values 
    (%s, %s, %s, %s, %s)
on conflict (user_id)
do update
set 
    first_name = excluded.user_id,
    last_name = excluded.last_name,
    gender = excluded.gender,
    level = excluded.level;
""")

song_table_insert = ("""insert into songs (song_id, title, artist_id, year, duration) values 
    (%s, %s, %s, %s, %s)""")

artist_table_insert = ("""insert into artists (artist_id, name, location, latitude, longitude) values
    (%s, %s, %s, %s, %s)
on conflict (artist_id)
do update 
set 
    artist_id = excluded.artist_id, 
    name = excluded.name, 
    location = excluded.location, 
    latitude = excluded.latitude, 
    longitude = excluded.longitude
""")


time_table_insert = ("""insert into time (start_time, hour, day, week, month, year, weekday) values
    (%s, %s, %s, %s, %s, %s, %s)
""")

# FIND SONGS

song_select = ("""
    select 
        songs.song_id, 
        songs.artist_id 
    from songs
    left join artists on songs.artist_id = artists.artist_id
    where songs.title = %s 
        and artists.name = %s
        and songs.duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]