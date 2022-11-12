import configparser
from library import db_client


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    client = db_client.DbClient(config)

    client.drop_schemas()
    client.create_schemas()
    client.create_tables()

    client.close_connection()


if __name__ == "__main__":
    main()