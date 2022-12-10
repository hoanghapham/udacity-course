import configparser
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, monotonically_increasing_id
from pyspark.sql.types import TimestampType
from pyspark import SparkConf
 
from . import custom_logger


logger = custom_logger.init_logger(__file__)

cred = configparser.ConfigParser()
cred.read("credentials/cred.cfg")

# Functions to create & config spark session
def create_spark_session():
    logger.info("Creating spark session...")
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.10.1") \
        .getOrCreate()
    spark.sparkContext.addPyFile("utils.zip")
    return spark


def config_spark_session(cred):
    conf = SparkConf()
    conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider')
    conf.set('spark.hadoop.fs.s3a.access.key', cred["AWS"]["AWS_ACCESS_KEY_ID"])
    conf.set('spark.hadoop.fs.s3a.secret.key', cred["AWS"]["AWS_SECRET_ACCESS_KEY"])
    conf.set('spark.hadoop.fs.s3a.session.token', cred["AWS"]['AWS_SESSION_TOKEN'])

    return conf


# Helper functions to convert timestamp from interger type to timestamp
def to_timestamp(timestamp: int) -> datetime:
    return datetime.utcfromtimestamp(timestamp / 1e3)

udf_to_timestamp = udf(lambda x: to_timestamp(x), TimestampType())
    

# Data processing functions
def generate_songs_data(data: DataFrame) -> DataFrame:
    """Extract only song data from the source song dataframe

    Parameters
    ----------
    data : DataFrame
        The source song dataframe

    Returns
    -------
    DataFrame
        A DataFrame containing only songs data
    """
    logger.info('Process songs data...')
    output = data.select("song_id", "title", "artist_id", "year", "duration")

    return output
    

def generate_artists_data(data: DataFrame) -> DataFrame:
    """Extract artists data from the source song dataframe

    Parameters
    ----------
    data : DataFrame
        The source song dataframe

    Returns
    -------
    DataFrame
        A DataFrame containing only artists data
    """
    logger.info('Process artists data...')
    output = data.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude")
    
    return output


def generate_users_data(data: DataFrame) -> DataFrame:
    """Extract users data from the source song dataframe

    Parameters
    ----------
    data : DataFrame
        The source song dataframe

    Returns
    -------
    DataFrame
        A DataFrame containing only users data
    """    
    logger.info('Process users data...')
    output = data \
        .select("userId", "firstName", "lastName", "gender", "level") \
        .withColumnRenamed("userId", "user_id") \
        .withColumnRenamed("firstName", "first_name") \
        .withColumnRenamed("lastName", "last_name") \
        .dropDuplicates()
    
    return output
    
    

def generate_time_data(data: DataFrame) -> DataFrame:
    """Extract time data from the source log dataframe

    Parameters
    ----------
    data : DataFrame
        The source log dataframe

    Returns
    -------
    DataFrame
        A DataFrame containing only time data
    """    
    logger.info('Process time data...')
    output = data.select("ts") \
        .withColumn("start_time", udf_to_timestamp(col("ts"))) \
        .withColumn("hour", hour(col("start_time"))) \
        .withColumn("day", dayofmonth(col("start_time"))) \
        .withColumn("week", weekofyear(col("start_time"))) \
        .withColumn("month", month(col("start_time"))) \
        .withColumn("year", year(col("start_time"))) \
        .withColumn("weekday", date_format(col("start_time"), "E"))
    
    return output
    

def load_song_data(spark: SparkSession, song_data_path: str) -> DataFrame:
    """Load songs data from source

    Parameters
    ----------
    spark : SparkSession
    song_data_path : str
        Full path, or path pattern to load songs data

    Returns
    -------
    DataFrame
        A DataFrame containing source songs data
    """    
    logger.info("Load song data...")
    song_df = spark.read.json(song_data_path)
    return song_df


def load_log_data(spark: SparkSession, log_data_path: str) -> DataFrame:
    """Load log data from source

    Parameters
    ----------
    spark : SparkSession
    log_data_path : str
        Full path, or path path to load log data

    Returns
    -------
    DataFrame
        A DataFrame containing source log data
    """
    logger.info("Load log data...")
    log_df = spark.read.json(log_data_path)
    return log_df


def generate_songplays_data(
        song_df: DataFrame, 
        log_df: DataFrame, 
        time_df: DataFrame, 
    ) -> DataFrame:
    """Create songplays data from songs, log and time dataframes

    Parameters
    ----------
    song_df : DataFrame
        DataFrame containing songs data process by `generate_songs_data()`
    log_df : DataFrame
        DataFrame containing log data process by `generate_log_data()`
    time_df : DataFrame
        dataframe containing time data process `generate_time_data()`

    Returns
    -------
    DataFrame
        A DataFrame containing songplays data - for analytics.
    """
    logger.info("Processing songplays data...")
    output = log_df \
        .join(song_df.select("title", "song_id", "artist_id"), song_df.title == log_df.song, "left") \
        .join(time_df, log_df.ts == time_df.ts, "left") \
        .withColumn("songplay_id", monotonically_increasing_id()) \
        .select("songplay_id", "start_time", "year", "month", "userId", "level", "song_id", "artist_id", "sessionId", "location", "userAgent") \
        .withColumnRenamed("userId", "user_id") \
        .withColumnRenamed("sessionId", "session_id") \
        .withColumnRenamed("userAgent", "user_agent")
    
    return output


def write_songs_table(data: DataFrame, output_path: str):
    logger.info("Writing to songs table...")
    data.write.parquet(output_path + "songs.parquet", mode = "overwrite")

def write_artists_table(data: DataFrame, output_path: str):
    logger.info("Writing to artists table...")
    data.write.parquet(output_path + "artists.parquet", mode = "overwrite")

def write_users_table(data: DataFrame, output_path: str):
    logger.info("Writing to users table...")
    data.write.parquet(output_path + "users.parquet", mode = "overwrite")

def write_time_table(data: DataFrame, output_path: str):
    logger.info("Writing to time table...")
    data.write.partitionBy("year", "month").parquet(output_path + "time.parquet", mode = "overwrite")

def write_songplays_table(data: DataFrame, output_path: str):
    logger.info("Writing to songplays table...")
    data.write.partitionBy("year", "month").parquet(output_path + "songplays.parquet", mode = "overwrite")

