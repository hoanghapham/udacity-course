
songplays_table_create_no_sort_dist = ("""
CREATE TABLE analytics.songplays_no_sort_dist (
    songplay_id         INTEGER IDENTITY(0, 1), 
    ts                  BIGINT,
    start_time          TIMESTAMP, 
    user_id             INTEGER, 
    level               VARCHAR(10), 
    song_id             VARCHAR, 
    artist_id           VARCHAR, 
    session_id          INTEGER, 
    location            VARCHAR, 
    users_agent         VARCHAR(max)
)
""")

songplays_table_insert_no_sort_dist = ("""
INSERT INTO analytics.songplays_no_sort_dist (
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

# Test queries
# Test sorting by different columns
test_query_1a = """
select
    ts
    , count(*)
from analytics.songplays
group by 1
order by 2 desc
"""

test_query_1b = """
select
    start_time
    , count(*)
from analytics.songplays
group by 1
order by 2 desc
"""

# Test sorting between sorted & no sort tables
test_query_2a = """
select
    ts
    , count(*)
from analytics.songplays
group by 1
order by 1 desc
"""

test_query_2b = """
select
    ts
    , count(*)
from analytics.songplays_no_sort_dist
group by 1
order by 1
"""

# Test join & aggregate between two tables
test_query_3a = """
select
    times.weekday
    , sum(songs.duration) / count(distinct times.date) as avg_daily_listen_duration
from analytics.songplays
left join analytics.songs on songplays.song_id = songs.song_id
left join analytics.times on songplays.ts = times.ts
group by 1
order by 1
"""

test_query_3b = """
select
    times.weekday
    , sum(songs.duration) / count(distinct times.date) as avg_daily_listen_duration
from analytics.songplays_no_sort_dist songplays
left join analytics.songs on songplays.song_id = songs.song_id
left join analytics.times on songplays.ts = times.ts
group by 1
order by 1
"""

test_query_4a = """
select
    songplays.artist_id
    , sum(songs.duration) total_listen_duration
from analytics.songplays
left join analytics.songs on songplays.song_id = songs.song_id
group by 1
order by 1
"""

test_query_4b = """
select
    songplays.artist_id
    , sum(songs.duration) total_listen_duration
from analytics.songplays_no_sort_dist songplays
left join analytics.songs on songplays.song_id = songs.song_id
group by 1
order by 1
"""

test_query_5a = """
select
    date(start_time) as date,
    count(*)
from analytics.songplays
group by 1
order by 1 desc
"""

test_query_5a = """
select
    date(start_time) as date,
    count(*)
from analytics.songplays_no_sort_dist
group by 1
order by 1 desc
"""