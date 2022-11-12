import configparser
import psycopg2
from library.custom_logger import init_logger
import library.sql_queries as queries

logger = init_logger(__name__)

class DbClient():
    """Convenience class to interact with the database
    """    

    def __init__(self, config:configparser.ConfigParser) -> None:
        self.config = config
        self._conn = self.init_connection()
        self._cur = self._conn.cursor() 

    def init_connection(self):
        logger.info('Initiating connection to the database...')
        conn = psycopg2.connect("""
            host={host} port={db_port} dbname={db_name} user={db_user} password={db_password}
            """.format(
                host=self.config['CLUSTER']['HOST'],
                db_port=self.config['CLUSTER']['DB_PORT'],
                db_name=self.config['CLUSTER']['DB_NAME'],
                db_user=self.config['CLUSTER']['DB_USER'],
                db_password=self.config['CLUSTER']['DB_PASSWORD']
            )
        )
        return conn

    def close_connection(self):
        self._conn.close()


    def execute_query(self, query: str) -> list:
        """Convenience method to execute a query safely, with error handling.

        Args:
            query (str): A query string

        Returns:
            list: If the query is a SELECT query, returns a list of returned rows. 
                If it is a DML like CREATE TABLE, INSERT, DELETE... returns a blank list 
        """
        try:
            self._cur.execute(query)
            self._conn.commit()
        except Exception as e:
            logger.error(e)
            self._conn.rollback()
            raise e

        else:
            try:
                result = self._cur.fetchall()
                return result
            except Exception as e:
                return None

    
    def drop_tables(self):
        logger.info("Dropping tables...")
        for i, query in enumerate(queries.drop_table_queries, start=1):
            self.execute_query(query)
            logger.info("Table {} done.".format(i))

    def drop_schemas(self):
        logger.info("Dropping schemas...")
        for i, query in enumerate(queries.drop_schema_queries, start=1):
            self.execute_query(query)
            logger.info("Schema {} done.".format(i))

    def create_schemas(self):
        logger.info("Creating schemas...")
        for i, query in enumerate(queries.create_schema_queries, start=1):
            self.execute_query(query)
            logger.info("Schema {} done.".format(i))

    def create_tables(self):
        logger.info("creating tables...")
        for i, query in enumerate(queries.create_table_queries, start=1):
            self.execute_query(query)
            logger.info("Table {} done.".format(i))

    def load_staging_tables(self):
        logger.info("Loading staging tables...")
        for i, query in enumerate(queries.copy_table_queries, start=1):
            self.execute_query(query)
            logger.info("Table {} done.".format(i))

    def insert_tables(self):
        logger.info('Transforming & inserting data to analytics tables...')
        for i, query in enumerate(queries.insert_table_queries, start=1):
            self.execute_query(query)
            logger.info("Table {} done".format(i))
