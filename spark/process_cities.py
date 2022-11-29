#%%
import csv
import re
from copy import copy
import argparse
from pyspark.sql import SparkSession

# %%

def coordinate_parser(coord_str):
    pattern = "\((.*)\)"
    result = re.findall(pattern, coord_str)
    long, lat = (float(i) for i in result[0].split(' '))

    return long, lat


def row_process(row):
    new_row = copy(row)
    long, lat = coordinate_parser(row['coords'])
    country_cap = row['country'].capitalize()

    new_row['country'] = country_cap
    new_row['long'] = long
    new_row['lat'] = lat

    return new_row
#%%

input = "s3://hapham/cities.csv"
output = "s3://hapham/new_cities.csv"

if __name__ == '__main__':
    # parser = argparse.ArgumentParser()
    # parser.add_argument('--input')
    # parser.add_argument('--output')

    # args = parser.parse_args()

    spark = SparkSession.builder.appName("Process cities").getOrCreate()

    cities = spark.read.option("header", 'true').csv(input)

    cities.createOrReplaceTempView('cities')

    new_cities = spark.sql("""
        select
            id
            , name
            , coords
            , start_year
            , url_name
            , initcap(country) as country
            , country_state
        from cities
    """
    )

    new_cities.write.option("header", "true").csv(output)

