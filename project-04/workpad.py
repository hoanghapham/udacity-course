#%%
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType
import pathlib

#%%
config = configparser.ConfigParser()
config.read('credentials/aws.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

project_folder = str(pathlib.Path('.').resolve())

# %%

# song_input_path = "s3://udacity-dend/song_data"
# log_input_path = "s3://udacity-dend/log_data"

log_input_path = "data/log_data"
song_input_path = "data/song_data/A"
p = pathlib.Path('.') / "data/song_data"
song_files = [str(i) for i in p.glob('**/*.json')]


#%%

spark_config = (
            ("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")
            , ('spark.executor.memory', '4G')
            , ('spark.driver.memory', '45G')
            , ('spark.driver.maxResultSize', '10G'))

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.driver.memory", "4G") \
        .config("spark.executor.memory", "4G") \
        .getOrCreate()
    return spark
# %%

spark = create_spark_session()
# %%

log_df = spark.read.json(log_input_path)
song_df = spark.read.json(song_files)
# %%

song_tbl = song_df.select('song_id', 'title', 'artist_id', 'year', 'duration')

# %%
song_tbl.write.parquet(project_folder + "/tables/songs.parquet", mode = 'overwrite')
# %%

artist_tbl = song_df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude')

# %%
artist_tbl.write.parquet(project_folder + "tables/artists.parquet", mode = 'overwrite')

# %%
users_tbl = log_df \
    .select('userId', 'firstName', 'lastName', 'gender', 'level') \
    .withColumnRenamed('userId', 'user_id') \
    .withColumnRenamed('firstName', 'first_name') \
    .withColumnRenamed('lastName', 'last_name') \
    .dropDuplicates()

# %%
users_tbl.write.parquet(project_folder + "/tables/users.parquet", mode = 'overwrite')

# %%

def to_timestamp(timestamp:int) -> datetime:
    return datetime.utcfromtimestamp(timestamp / 1e3)

udf_to_timestamp = udf(lambda x: to_timestamp(x), TimestampType())

time_tbl = log_df.select('ts') \
    .withColumn('start_time', udf_to_timestamp(col('ts'))) \
    .withColumn('hour', hour(col('start_time'))) \
    .withColumn('day', dayofmonth(col('start_time'))) \
    .withColumn('week', weekofyear(col('start_time'))) \
    .withColumn('month', month(col('start_time'))) \
    .withColumn('year', year(col('start_time'))) \
    .withColumn('weekday', date_format(col('start_time'), 'E'))

# %%

time_tbl.write.partitionBy('year', 'month').parquet(project_folder + "/tables/time.parquet", mode = 'overwrite')

# %%
# songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

songplays_tbl = log_df \
    .join(song_tbl.select('title', 'song_id', 'artist_id'), song_tbl.title == log_df.song, 'left') \
    .join(time_tbl, log_df.ts == time_tbl.ts, 'left') \
    .withColumn('songplay_id', monotonically_increasing_id()) \
    .select('songplay_id', 'start_time', 'year', 'month', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent') \
    .withColumnRenamed('userId', 'user_id') \
    .withColumnRenamed('sessionId', 'session_id') \
    .withColumnRenamed('userAgent', 'user_agent')

# %%

songplays_tbl.write.partitionBy('year', 'month').parquet(project_folder + "/tables/songplays.parquet", mode = 'overwrite')
# %%
