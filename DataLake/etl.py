import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
import  pyspark.sql.functions as F
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import expr

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create Spark session.
        
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Read songs data from S3. Extract and trasform songs data to create songs_table and artists_table and store them bak to s3 output bucket in parquet format.
        
        Parameters:
            spark: spark session
            input_data: song data directory on s3 
            output_data: output directory for the created songs_table and artists_table
    """
    # get filepath to song data file
    song_data_path = os.path.join(input_data, "song_data/*/*/*/*.json")
    
    # read song data file
    df = spark.read.json(song_data_path)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")
    #cast year from long to int
    songs_table = songs_table.withColumn("year", expr("cast(year as int)"))
    
    # write songs table to parquet files partitioned by year and artist   
    songs_table.write.partitionBy("year", "artist_id").mode("overwrite").parquet(output_data + "/songs_table")

    # extract columns to create artists table
    artists_table = df.selectExpr("artist_id", "artist_name as name", "artist_location as location", "artist_latitude as latitude", "artist_longitude as longitude")
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + "/artists_table")
    
    return (songs_table, artists_table)


def process_log_data(spark, input_data, output_data, songs_table, artists_table):
    """
    Read logs data from S3. Extract and trasform songs data to create songs_table and artists_table and store them bak to s3 output bucket in parquet format.
        
        Parameters:
            spark: spark session
            input_data: song data directory on s3 
            output_data: output directory for the created songs_table and artists_table
            songs_table: songs_table dataFrame for creating songplays_table
            artists_table: artist_table dataFrame for creating songplays_table
    """
    # get filepath to log data file
    log_data_path = os.path.join(input_data,"log_data")

    # read log data file
    df = spark.read.json(log_data_path)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.selectExpr(["userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"]).drop_duplicates()
    #cast user_id from string to int
    users_table = users_table.withColumn("user_id", expr("cast(user_id as int)"))
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + "/users_table")  

    # create timestamp column from original timestamp column. The following code somehow didn't work
    #get_timestamp = udf(lambda x: datetime.to_timestamp(x))
    #df = df.withColumn("ts", get_timestamp("ts"))
                                   
    # create datetime column from original timestamp column
    df = df.withColumn("start_time", F.from_unixtime(F.col("ts")/1000))
    
    # extract column to create time df
    time_df = df.select("start_time").drop_duplicates()
    
    #create columns from start_time to create time table
    time_table = time_df.selectExpr(["start_time as start_time", "hour(start_time) as hour", "dayofmonth(start_time) as day", "weekofyear(start_time) as week", "month(start_time) as month", "year(start_time) as year", "dayofweek(start_time) as weekday"])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month").mode("overwrite").parquet(output_data + "/time_table")

    # read in song data to use for songplays table
    # get filepath to song data file
    
    # create temp view of songs_table, artists_table and df
    songs_table.createOrReplaceTempView("songs")
    artists_table.createOrReplaceTempView("artists")                               
    df.createOrReplaceTempView("logs")

    # extract columns from joined songs, artists and logs dataframes to create songplays table 
    songplays_table = spark.sql("""
        select
            row_number() over (order by start_time) as songplay_id,
            start_time,
            userId as user_id,
            level,
            s.song_id,
            a.artist_id,
            sessionId as session_id, 
            l.location, 
            userAgent as user_agent,
            year(start_time) as year,
            month(start_time) as month
        from 
            logs l
        join
            songs s
        on
            l.song=s.title
            and
            l.length=s.duration
        join
            artists a
        on
            l.artist=a.name
            
        
    """)
    songplays_table = songplays_table.withColumn("session_id", expr("cast(session_id as int)"))
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month").mode("overwrite").parquet(output_data + "/songplays_table")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dlprojdata/"
    
    songs_table, artists_table = process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data, songs_table, artists_table)


if __name__ == "__main__":
    main()
