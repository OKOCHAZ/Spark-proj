import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['Credintials']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['Credintials']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path(input_data, 'data/song-data.zip')
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = spark.sql(""""SELECT songs.title,
                songs.artist_id,
                songs.year,
                songs.duration,
                row_number() as song_id
                FROM songs""")
    songs_table.dropDublicats(['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs.parquet'), 'overwrite')

    # extract columns to create artists table
    artists_table = spark.sql(""""SELECT songs.artists_id,
                    songs.artist_name as name,
                    songs.artist_location as location,
                    songs.artist_latitude as latitude,
                    songs.artist_longitude as longitude
                    FROM songs""")
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists.parquet'), 'overwrite')



def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path(input_data, 'data/log-data.zip')

    # read log data file
    df = spark.read.csv(log_data)
    
    # filter by actions for song plays
    df = df['songplay_id', 'userId', 'level' ,'song_id', 'artist_id' , 'sessionId', 'location' , 'userAgent']

    # extract columns for users table    
    users_table = spark.sql(""""SELECT logs.userId as user_id,
                    logs.firstName as fisrt_name,
                    logs.lastName as last_name,
                    logs.gender,
                    logs.level
                    FROM logs""")
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users.parquet'), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn('time_stamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x:  str(datetime.fromtimestamp(int(x) / 1000.0)))
    df = df.withColumn('date_time' , get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select(
                    col('datetime').alias('start_time'),
                    hour('datetime').alias('hour'),
                    dayofmonth('datetime').alias('day'),
                    weekofyear('datetime').alias('week'),
                    month('datetime').alias('month'),
                    year('datetime').alias('year'),
                    weekday('datetime').alias('weekday'))
    
    time_table= time_table.dropDublicates(['start_time'])
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'time.parquet'), 'overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    df = df.join(song_df, song_df.title == df.song)
    
    songplays_table = spark.sql(""""SELECT logs.ts as start_time,
                    log.user_id, logs.level, songs.song_id,
                    songs.artist_id, logs.sessionId, logs.location,
                    logs.userAgent,
                    year(logs.timestamp) as year,
                    month(logs.timestamp) as month
                    row_number() as songplay_id,
                    FROM logs JOIN songs ON songs.artist_name = logs.artist
                    and log.song = songs.title""")
    
    songplays_table = songplays_table.selectExpr("ts as start_time")
    songplays_table.select(monotonically_increasing_id().alias('songplay_id')).collect()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'songplays.parquet'), 'overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://mawhawk/proj/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
