import os
from configparser import ConfigParser
from utils import tasks
from utils import custom_logger


logger = custom_logger.init_logger(__file__)

def main():

    config = ConfigParser()
    config.read("configs/config.cfg")

    song_data_path = config['DATA']['SONG_DATA_PATH']
    log_data_path = config['DATA']['LOG_DATA_PATH']
    output_path = config['DATA']['OUTPUT_PATH']
    
    logger.info("Preparing...")
    spark = tasks.create_spark_session()
    source_song_df = tasks.load_song_data(spark, song_data_path)
    source_log_df = tasks.load_log_data(spark, log_data_path)


    logger.info("Processing songs data...")
    songs_df = tasks.generate_songs_data(source_song_df)
    tasks.write_songs_table(songs_df, output_path)

    artists_df = tasks.generate_artists_data(source_song_df)
    tasks.write_artists_table(artists_df, output_path)


    logger.info("Processing log data...")
    users_table = tasks.generate_users_data(source_log_df)
    tasks.write_users_table(users_table, output_path)

    time_df = tasks.generate_time_data(source_log_df)
    tasks.write_time_table(time_df, output_path)

    songplays_df = tasks.generate_songplays_data(songs_df, source_log_df, time_df)
    tasks.write_songplays_table(songplays_df, output_path)



if __name__ == "__main__":
    main()
