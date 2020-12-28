import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id, date_format
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear
from pyspark.sql.types import TimestampType, DateType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']

default_song_subpath = '/*/*/*/*'
default_log_subpath = '/*'

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """
    :param spark: spark session to process data
    :param input_data: s3 bucket where contains song data files
    :param output_data: file path where result of this function will save files
    """
    
    # get filepath to song data file
    song_data = f"{input_data}{default_song_subpath}"
    
    # read song data file
    df = spark.read.json(song_data).drop_duplicates()

    # extract columns to create songs table
    songs_table = (df.select("song_id",
                             "title",
                             "artist_id",
                             "year",
                             "duration").drop_duplicates())
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(f"{output_data}/songs/", mode="overwrite", partitionBy=["year","artist_id"])

    # extract columns to create artists table
    artists_table = (df.select("artist_id","artist_name","artist_location","artist_latitude","artist_longitude")
                       .drop_duplicates())
    
    # write artists table to parquet files
    artists_table.write.parquet(f"{output_data}/artists/", mode="overwrite")
    
    return df, songs_table, artists_table

def process_log_data(spark, input_data, output_data):
    """
    :param spark: spark session to process data
    :param input_data: s3 bucket where contains log data files
    :param output_data: file path where result of this function will save files
    """
    
    # get filepath to log data file
    log_data = f"{input_data}{default_log_subpath}"

    # read log data file
    df = spark.read.json(log_data, mode='PERMISSIVE').drop_duplicates()
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table = (df.select("userId","firstName","lastName","gender","level")
                     .drop_duplicates())
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(f"{output_data}/users/") , mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : datetime.utcfromtimestamp(int(x)/1000), TimestampType())
    df = df.withColumn("start_time", get_timestamp("ts")) 
    
    # create datetime column from original timestamp column
    get_date = udf(lambda x : datetime.utcfromtimestamp(int(x)/1000), DateType())
    df = df.withColumn("start_date", get_date("ts"))
    
    # create year and month columns to use as partition key in songplays_table
    df = (df.withColumn("datetime_year",year("start_time"))
            .withColumn("datetime_month",month("start_time")))
    
    # extract columns to create time table
    time_table = (df.withColumn("hour",hour("start_time"))
                    .withColumn("day",dayofmonth("start_time"))
                    .withColumn("week",weekofyear("start_time"))
                    .withColumnRenamed("datetime_year","year")
                    .withColumnRenamed("datetime_month","month")
                    .select("start_time","hour", "day", "week", "month", "year").drop_duplicates())
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(f"{output_data}/time_table/"), mode='overwrite', partitionBy=["year","month"])

    # read in song data to use for songplays table
    song_df = (spark.read.format("parquet")
                 .option("basePath", os.path.join(output_data, "songs/"))
                 .load(os.path.join(f"{output_data}/songs{default_log_subpath}/")))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = (df.join(song_df, df.song == song_df.title, how='inner')
                         .select(monotonically_increasing_id().alias("songplay_id"),
                                 df.start_time,
                                 df.userId,
                                 df.level,
                                 song_df.song_id,
                                 song_df.artist_id,
                                 df.sessionId,
                                 df.location,
                                 df.userAgent,
                                 df.datetime_year,
                                 df.datetime_month))

    # write songplays table to parquet files partitioned by year and month
    (songplays_table.withColumnRenamed("datetime_year","year")
                    .withColumnRenamed("datetime_month","month")
                    .withColumnRenamed("userId","user_id")
                    .withColumnRenamed("sessionId","session_id")
                    .withColumnRenamed("userAgent","user_agent")
                    .drop_duplicates()
                    .write.parquet(os.path.join(f"{output_data}/songplays/"),
                                  mode="overwrite",
                                  partitionBy=["year","month"]))
    
    return df, users_table, time_table, song_df, songplays_table

def main():
    sk = create_spark_session()
    input_song_data = 's3a://singaretti/song_data'
    input_log_data = 's3a://singaretti/log_data'
    output_data = 's3a://singaretti/output'
    
    process_song_data(spark, input_song_data, output_data)    
    process_log_data(spark, input_log_data, output_data)

if __name__ == "__main__":
    main()
