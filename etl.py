import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import from_unixtime
from pyspark.sql.types import IntegerType
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Return a new Spark Session object configured to work with AWS."""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Process song data with spark and store output.

    Keyword arguments:
    spark -- spark session object
    input_data -- filepath to input data files
    output_data -- filepath to store output data files
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', \
                            'title', \
                            'artist_id', \
                            'year', \
                            'duration').distinct()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(path = output_data + 'songs/', \
                              partitionBy = ('year', 'artist_id'))

    # extract columns to create artists table
    artists_table = df.select('artist_id', \
                              'artist_name', \
                              'artist_location', \
                              'artist_latitude', \
                              'artist_longitude').distinct()

    # write artists table to parquet files
    artists_table.write.parquet(path = output_data + 'artists/')


def main():
    """Start a spark session and process song and log data."""
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://ucourse-dend-spark/"


if __name__ == "__main__":
    main()
