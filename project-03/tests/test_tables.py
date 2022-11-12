import configparser
from library import db_client
from library import sql_queries as queries
from library.custom_logger import init_logger

logger = init_logger(__name__)

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    client = db_client.DbClient(config)

    logger.info("staging_events_table_check. Rows count: {:,}".format(client.execute_query(queries.staging_events_table_check)[0][0]))
    logger.info("staging_songs_table_check. Rows count: {:,}".format(client.execute_query(queries.staging_songs_table_check)[0][0]))
    logger.info("songplays_table_check. Rows count: {:,}".format(client.execute_query(queries.songplays_table_check)[0][0]))
    logger.info("users_table_check. Rows count: {:,}".format(client.execute_query(queries.users_table_check)[0][0]))
    logger.info("songs_table_check. Rows count: {:,}".format(client.execute_query(queries.songs_table_check)[0][0]))
    logger.info("artists_table_check. Rows count: {:,}".format(client.execute_query(queries.artists_table_check)[0][0]))
    logger.info("time_table_check. Rows count: {:,}".format(client.execute_query(queries.time_table_check)[0][0]))

    client.close_connection()


if __name__ == "__main__":
    main()