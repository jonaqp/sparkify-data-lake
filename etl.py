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


def process_log_data(spark, input_data, output_data):
    """Process log data with spark and store output.

    Keyword arguments:
    spark -- spark session object
    input_data -- filepath to input data files
    output_data -- filepath to store output data files
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.select('*').where(df['page'] == 'NextSong')

    # extract columns for users table
    users_table = df.select(df['userId'].alias('user_id'), \
                        df['firstName'].alias('first_name'), \
                        df['lastName'].alias('last_name'), \
                        df['gender'], \
                        df['level']).distinct()

    # write users table to parquet files
    users_table.write.parquet(path = output_data + 'users/')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x/1000, IntegerType())
    df = df.withColumn('start_time', get_timestamp('ts'))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: from_unixtime(x), TimestampType())
    df = df.withColumn('datetime', from_unixtime('start_time'))

    # extract columns to create time table
    time_table = df.select('start_time', \
                       hour('datetime').alias('hour'), \
                       dayofmonth('datetime').alias('day'), \
                       weekofyear('datetime').alias('week'), \
                       month('datetime').alias('month'), \
                       year('datetime').alias('year'), \
                       date_format('datetime', 'u').alias('weekday'))

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(path = output_data + 'time/', \
                             partitionBy = ('year', 'month'))

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(song_df, df['song'] == song_df['title']) \
                    .select(monotonically_increasing_id().alias('songplay_id'),
                            'start_time',
                            year('datetime').alias('year'),
                            month('datetime').alias('month'),
                            df['userId'].alias('user_id'),
                            'level',
                            'song_id',
                            'artist_id',
                            df['sessionId'].alias('session_id'),
                            'location',
                            df['userAgent'].alias('user_agent'))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(path = output_data + 'songplays/', \
                              partitionBy = ('year', 'month'))


def main():
    """Start a spark session and process song and log data."""
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://ucourse-dend-spark/"


if __name__ == "__main__":
    main()
