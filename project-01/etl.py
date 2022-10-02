import os
import glob
import psycopg2
import pandas as pd
import sql_queries as queries


def process_song_file(cur, filepath):
    """Read in song file, and then insert data to `songs` and `artists` tables

    Parameters
    ----------
    cur : psycopg2.extensions.cursor
        Cursor object created from a PostgreSQL connection
    filepath : str
        Path to the song file.
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].drop_duplicates()

    for idx, row in song_data.iterrows():
        try:
            cur.execute(queries.song_table_insert, row)
        except Exception as e:
            raise e
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].drop_duplicates()
    for idx, row in artist_data.iterrows():
        try:
            cur.execute(queries.artist_table_insert, row)
        except Exception as e:
            raise e


def process_log_file(cur, filepath):
    """Load log file, and then insert data to `time`, `users` and `songplays` tables.

    Parameters
    ----------
    cur : psycopg2.extensions.cursor
        Cursor object created from a PostgreSQL connection
    filepath : str
        Path to the log file.
    """
    # open log file
    log_df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    time_df = log_df.loc[log_df.page == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(log_df.ts, unit='ms')
    
    # insert time data records
    time_data = zip(
        t, 
        t.dt.hour, 
        t.dt.day, 
        t.dt.isocalendar().week, 
        t.dt.month, 
        t.dt.year, 
        t.dt.weekday
    )
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday') 
    time_df = pd.DataFrame(time_data, columns=column_labels)


    for i, row in time_df.iterrows():
        cur.execute(queries.time_table_insert, list(row))

    # load user table
    # When the user changes their information, there will be multiple rows for one userId.
    # In that case, use the latest info to update the user's record.
    # This logic is handled in sql_queries.user_table_insert

    user_df_base = log_df[['userId', 'firstName', 'lastName', 'gender', 'level', 'ts']].drop_duplicates()
    user_df = user_df_base \
        .groupby(['userId', 'firstName', 'lastName', 'gender', 'level']) \
        .max('ts') \
        .sort_values(['userId', 'ts']) \
        .reset_index()

    # insert user records
    for i, row in user_df.iterrows():
        insert_values = (
            row.userId, 
            row.firstName, 
            row.lastName, 
            row.gender, 
            row.level
        )
        cur.execute(queries.user_table_insert, insert_values)

    # insert songplay records
    for index, row in log_df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(queries.song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (
            pd.to_datetime(row.ts, unit='ms'), 
            row.userId, 
            row.level,
            songid,
            artistid,
            row.sessionId,
            row.location,
            row.userAgent
        )
        cur.execute(queries.songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """Generic function serves as the interface to call different 
    data processing functions.

    Parameters
    ----------
    cur : psycopg2.extensions.cursor
        Cursor object created from a PostgreSQL connection
    conn : psycopg2.extensions.connection
        Connection object created from psycopg2
    filepath : str
        Path to the file to be processed
    func : function
        A data processing function
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    host = "127.0.0.1"
    port = "5432"

    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname="sparkifydb", 
        user="student",
        password="student"
    )
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()