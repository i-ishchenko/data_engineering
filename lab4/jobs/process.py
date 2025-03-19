import os
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, to_date, month, col, row_number, date_sub
from pyspark.sql.window import Window

def average_trip_duration_by_day(df):
    df_with_date = df.withColumn("date", to_date("start_time"))
    result = df_with_date.groupBy("date").agg(avg("tripduration").alias("avg_tripduration"))
    return result

def trips_per_day(df):
    df_with_date = df.withColumn("date", to_date("start_time"))
    result = df_with_date.groupBy("date").agg(count("*").alias("trip_count"))
    return result

def most_popular_station_by_month(df):
    df_with_month = df.withColumn("month", month("start_time"))
    grouped = df_with_month.groupBy("month", "from_station_name").agg(count("*").alias("trip_count"))
    windowSpec = Window.partitionBy("month").orderBy(col("trip_count").desc())
    result = grouped.withColumn("rank", row_number().over(windowSpec)).filter(col("rank") == 1).drop("rank")
    return result

def top_three_stations_last_two_weeks(df):
    df_with_date = df.withColumn("date", to_date("start_time"))
    max_date = df_with_date.agg({"date": "max"}).collect()[0][0]
    last_two_weeks_date = (max_date - datetime.timedelta(days=14)).strftime("%Y-%m-%d")
    
    filtered = df_with_date.filter(col("date") >= last_two_weeks_date)
    grouped = filtered.groupBy("date", "from_station_name").agg(count("*").alias("trip_count"))
    windowSpec = Window.partitionBy("date").orderBy(col("trip_count").desc())
    result = grouped.withColumn("rank", row_number().over(windowSpec)) \
                    .filter(col("rank") <= 3) \
                    .drop("rank")
    return result


def average_duration_by_gender(df):
    result = df.groupBy("gender").agg(avg("tripduration").alias("avg_duration"))
    return result

def write_output(df, output_path):
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Lab4").getOrCreate()
   
    input_csv = "/opt/bitnami/spark/jobs/Divvy_Trips_2019_Q4.csv"
    df = df = spark.read.csv(input_path, header=True, inferSchema=True)
    
    out_dir = "/opt/bitnami/spark/out"
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    
    write_output(average_trip_duration_by_day(df), os.path.join(out_dir, "avg_trip_duration_per_day"))
    
    write_output(trips_per_day(df), os.path.join(out_dir, "trips_per_day"))
    
    write_output(most_popular_station_by_month(df), os.path.join(out_dir, "most_popular_station_by_month"))
    
    write_output(top_three_stations_last_two_weeks(df), os.path.join(out_dir, "top_three_stations_last_two_weeks"))
    
    write_output(average_duration_by_gender(df), os.path.join(out_dir, "average_duration_by_gender"))
    
    spark.stop()
