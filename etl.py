import configparser
import os
import glob
from pyspark.sql import SparkSession
import pandas as pd
import shutil

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get("AWS", 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get("AWS", 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):

    # get filepath to song data file
    song_data = get_files('data/song_data')
    # read song data file
    df = spark.read.json(song_data)
    df.printSchema()

    # SONGS TABLE
    df.createOrReplaceTempView("songs_table")
    columns = ['song_id', 'title', 'artist_id', 'year', 'duration']

    songs_table = \
    spark.sql(
    """
    SELECT song_id, title, artist_id, year, duration
    FROM songs_table
    """
    ).toDF(*columns)

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(os.path.join(output_data, "songs.parquet"), "overwrite")

    # ARTIST TABLE
    df.createOrReplaceTempView("artists_table")

    # extract columns to create artists table
    columns = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    artists_table = spark.sql(
        """
    SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM artists_table
    """).toDF(*columns)

    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, "artists.parquet"), "overwrite")


def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))
    return all_files


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = get_files('data/log_data')

    # read log data file
    df = spark.read.json(log_data)
    df.printSchema()
    # filter by actions for song plays
    df = df[df['page'] == 'NextSong']

    # USER TABLE
    # extract columns for users table
    df.createOrReplaceTempView("users_table")
    columns = ['userId', 'firstName', 'lastName', 'gender', 'level']

    # write users table to parquet files
    users_table = spark.sql(
        """
    SELECT userId, firstName, lastName, gender, level
    FROM users_table
    """).toDF(*columns)

    users_table.write.parquet(os.path.join(output_data, "users.parquet"), "overwrite")

    # TIME TABLE
    # create timestamp column from original timestamp column
    # get_timestamp = udf()
#     df =
#
#     # create datetime column from original timestamp column
#     get_datetime = udf()
#     df =
#
#     # extract columns to create time table
#     time_table =
#
#     # write time table to parquet files partitioned by year and month
#     time_table
#
#     # read in song data to use for songplays table
#     song_df =
#
#     # extract columns from joined song and log datasets to create songplays table
#     songplays_table =
#
#     # write songplays table to parquet files partitioned by year and month
#     songplays_table


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""

    # process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()