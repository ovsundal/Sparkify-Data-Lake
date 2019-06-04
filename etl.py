import configparser
import os
import glob
from pyspark.sql import SparkSession
import pandas as pd

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

    df = spark.read.text(song_data)

    pd.set_option('max_colwidth', 200)
    print(df.limit(5).toPandas())

    # df.createOrReplaceTempView("songs_table")

    # extract columns to create songs table
    # songs_table = df.withColumn("num_songs")
    # songs_table.printSchema()

    # songs_table = spark.sql(
    # """
    # SELECT *
    # FROM songs_table
    # """
    # ).show()

    # write songs table to parquet files partitioned by year and artist
    # songs_table

    # extract columns to create artists table
    # artists_table =

    # write artists table to parquet files
    # artists_table


def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))
    return all_files


# def process_log_data(spark, input_data, output_data):
#     # get filepath to log data file
#     log_data = 's3a://udacity-dend/log_data'
#
#     # read log data file
#     df =
#
#     # filter by actions for song plays
#     df =
#
#     # extract columns for users table
#     artists_table =
#
#     # write users table to parquet files
#     artists_table
#
#     # create timestamp column from original timestamp column
#     get_timestamp = udf()
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

    process_song_data(spark, input_data, output_data)
    # process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()