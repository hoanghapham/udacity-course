import configparser
from library import db_client


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    client = db_client.DbClient(config)
    
    client.load_staging_tables()
    client.insert_tables()

    client.close_connection()


if __name__ == "__main__":
    main()