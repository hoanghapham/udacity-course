import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, monotonically_increasing_id
from pyspark.sql.types import TimestampType
from utils import custom_logger

from pyspark import SparkConf
from pyspark.sql import SparkSession
 
config = configparser.ConfigParser()
config.read("dl.cfg")

conf = SparkConf()
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider')
conf.set('spark.hadoop.fs.s3a.access.key', config["AWS"]["AWS_ACCESS_KEY_ID"])
conf.set('spark.hadoop.fs.s3a.secret.key', config["AWS"]["AWS_SECRET_ACCESS_KEY"])
conf.set('spark.hadoop.fs.s3a.session.token', config["AWS"]['AWS_SESSION_TOKEN'])

logger = custom_logger.init_logger(__file__)


os.environ["AWS_ACCESS_KEY_ID"] = config["AWS"]["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"] = config["AWS"]["AWS_SECRET_ACCESS_KEY"]


def create_spark_session():
    logger.info("Creating spark session...")
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .getOrCreate()
    return spark

def to_timestamp(timestamp:int) -> datetime:
    return datetime.utcfromtimestamp(timestamp / 1e3)



def process_song_data(spark, input_path, output_path):
   
    logger.info("Load song data...")
    # read song data file
    # song_df = spark.read.json(input_path + "song_data/*/*/*")
    song_df = spark.read.json(input_path + "song_data/A/A/A")

    # extract columns to create songs table
    songs_table = song_df.select("song_id", "title", "artist_id", "year", "duration")
    
    # write songs table to parquet files partitioned by year and artist
    logger.info('Write songs table...')
    songs_table.write.parquet(output_path + "songs.parquet", mode = "overwrite")

    # extract columns to create artists table
    artists_table = song_df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude")
    
    # write artists table to parquet files
    logger.info('Write artists table...')
    artists_table.write.parquet(output_path + "artists.parquet", mode = "overwrite")


def process_log_data(spark, input_path, output_path):

    logger.info("Load log data...")
    # read log data file
    log_df = spark.read.json(input_path + "log_data/")

    # filter by actions for song plays
    log_df.filter(col("page") == "NextSong")

    # extract columns for users table    
    logger.info("Process users data...")
    users_table = log_df \
        .select("userId", "firstName", "lastName", "gender", "level") \
        .withColumnRenamed("userId", "user_id") \
        .withColumnRenamed("firstName", "first_name") \
        .withColumnRenamed("lastName", "last_name") \
        .dropDuplicates()
    
    # write users table to parquet files
    logger.info("Write users table...")
    users_table.write.parquet(output_path + "users.parquet", mode = "overwrite")

    # create timestamp column from original timestamp column
    udf_to_timestamp = udf(lambda x: to_timestamp(x), TimestampType())
    
    # extract columns to create time table
    logger.info("Process time data...")
    time_table = log_df.select("ts") \
        .withColumn("start_time", udf_to_timestamp(col("ts"))) \
        .withColumn("hour", hour(col("start_time"))) \
        .withColumn("day", dayofmonth(col("start_time"))) \
        .withColumn("week", weekofyear(col("start_time"))) \
        .withColumn("month", month(col("start_time"))) \
        .withColumn("year", year(col("start_time"))) \
        .withColumn("weekday", date_format(col("start_time"), "E"))
    
    # write time table to parquet files partitioned by year and month
    logger.info("Write time table...")
    time_table.write.partitionBy("year", "month").parquet(output_path + "time.parquet", mode = "overwrite")

    # read in song data to use for songplays table
    # song_df = spark.read.json(input_path + "song_data/*/*/*")
    song_df = spark.read.json(input_path + "song_data/A/A/A")


    # extract columns from joined song and log datasets to create songplays table 
    logger.info("Process songplays data...")
    songplays_table = log_df \
        .join(song_df.select("title", "song_id", "artist_id"), song_df.title == log_df.song, "left") \
        .join(time_table, log_df.ts == time_table.ts, "left") \
        .withColumn("songplay_id", monotonically_increasing_id()) \
        .select("songplay_id", "start_time", "year", "month", "userId", "level", "song_id", "artist_id", "sessionId", "location", "userAgent") \
        .withColumnRenamed("userId", "user_id") \
        .withColumnRenamed("sessionId", "session_id") \
        .withColumnRenamed("userAgent", "user_agent")

    # write songplays table to parquet files partitioned by year and month
    logger.info("Write songplays table...")
    songplays_table.write.partitionBy("year", "month").parquet(output_path + "songplays.parquet", mode = "overwrite")


def main():
    spark = create_spark_session()
    input_path = "s3a://udacity-dend/"
    # output_path = "s3a://hapham/tables/"
    # input_path = "data/"
    output_path = "./tables/"
    
    process_song_data(spark, input_path, output_path)    
    process_log_data(spark, input_path, output_path)

    logger.info("Finished!")

if __name__ == "__main__":
    main()
