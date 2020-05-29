import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, row_number, udf, dayofweek, col, monotonically_increasing_id
from pyspark.sql import Window
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Description: This function can be used to create the spark Session 
    
    Returns:
        sparkSession object
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description: This function can be used to read the file in the input_data (song_data)
    to get the song and artist info and used to populate the songs and artists dim tables.
    
    Arguments:
        input_data: the path where the input json files are present. 
        output_data: path where the otuput parquet files are written to
    
    Returns:
        None
    """
    
    # get filepath to song data file
    song_data = input_data+"song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data).drop_duplicates()

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').drop_duplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy('year','artist_id').parquet(output_data+'analytics/songs')

    # extract columns to create artists table
    artists_table = df.select(col('artist_id'), 
                              col('artist_name').alias('artist'), 
                              col('artist_location').alias('location'),
                              col('artist_latitude').alias('latitude'),
                              col('artist_longitude').alias('longitude')).drop_duplicates()
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data+'analytics/artists')


def process_log_data(spark, input_data, output_data):
    """
    Description: This function can be used to read the file in the filepath (log_data)
    to get the user, time, songplays info and used to populate the users and time dim tables and songplays fact table.
    
    Arguments:
        input_data: the path where the input json files are present. 
        output_data: path where the otuput parquet files are written to
    
    Returns:
        None
    """
    # get filepath to log data file
    log_data = input_data+"log_data/*.json"

    # read log data file
    log_df = spark.read.json(log_data).drop_duplicates()
    
    # filter by actions for song plays
    log_df = log_df.where(col("page")=="NextSong")

    # extract columns for users table    
    users_table = log_df.withColumn("rn", row_number().over(Window.partitionBy("userId").orderBy(col("ts").desc()))).where(col("rn")==1).\
                                                    select(col('userId'),
                                                           col('firstName').alias('first_name'),
                                                           col('lastName').alias('last_name'),
                                                           col('gender'),
                                                           col('level'))
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data+'analytics/users')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ms : datetime.utcfromtimestamp(ms/1000), TimestampType())
    log_df = log_df.withColumn("start_time", get_timestamp("ts"))
    
    # extract columns to create time table
    time_table = log_df.select('start_time').dropDuplicates().select('start_time',
                   hour('start_time').alias('hour'), 
                   dayofmonth('start_time').alias('day'), 
                   weekofyear('start_time').alias('week'),
                   month('start_time').alias('month'),
                   year('start_time').alias('year'),
                   dayofweek('start_time').alias('weekday'))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy('year','month').parquet(output_data+'analytics/time')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data+"song_data/*/*/*/*.json").drop_duplicates()

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = log_df.join(song_df,log_df.artist == song_df.artist_name).drop_duplicates().select(monotonically_increasing_id().alias('songplay_id'),\
                                                                                   'start_time', col('userId').alias('user_Id'), 'level',\
                                                                                   'song_id','artist_id',\
                                                                                   col('sessionId').alias('session_id'), 'location',\
                                                                                   col('userAgent').alias('user_agent'))
    
    songplays_table = songplays_table.join(time_table, songplays_table.start_time == time_table.start_time).select('songplay_id',\
                                                                                                                  songplays_table.start_time,\
                                                                                                                   'user_Id','level','song_id',\
                                                                                                                   'artist_id','session_id',\
                                                                                                                   'location','user_agent','year',\
                                                                                                                   'month').drop_duplicates()
    # write songplays table to parquet files partitioned by song_id (as year and month are not part of this table)
    songplays_table.write.mode("overwrite").partitionBy('year','month').parquet(output_data+'analytics/songplays')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    #input_data = ""
    output_data = "s3a://Sparkifydl/"
    #output_data = ""
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
