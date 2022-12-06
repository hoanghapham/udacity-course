#%%
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
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
song_tbl.write.parquet(project_folder + "/tables/songs.parquet")
# %%

artist_tbl = song_df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude')

# %%
artist_tbl.write.parquet(project_folder + "/tables/artists.parquet")
# %%
