from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Create a Spark session
spark = SparkSession.builder.appName("PlayerIntensity").getOrCreate()

# Read data from Parquet file using the absolute path
file_path = "/mounted-data/data/processed/trackable_object.parquet"
timeframe_path = "/mounted-data/data/processed/timeframe.parquet"

# Load trackable_object and timeframe data
df = spark.read.parquet(file_path)
df_time = spark.read.parquet(timeframe_path)

# Join dataframes to add timestamp
df = df.join(df_time, on=["match_id", "frame"], how="left")

# Drop rows with non-null z values
df = df.filter(df.z.isNull())

# Convert 'timestamp' to HH:mm:ss.SS format
df = df.withColumn(
    'timestamp',
    F.date_format(F.from_unixtime(F.unix_timestamp('timestamp', 'HH:mm:ss.SS')), 'HH:mm:ss.SS')
)

# Print the schema of the DataFrame
df.printSchema()

# Calculate euclidean distance between consecutive frames for each player
df = df.withColumn("distance", F.sqrt((F.col("x") - F.lag("x").over(Window.partitionBy("trackable_object", "match_id").orderBy("frame")))**2 +
                                      (F.col("y") - F.lag("y").over(Window.partitionBy("trackable_object", "match_id").orderBy("frame")))**2))

# Define a window specification for 5-minute intervals
five_minute_window = Window.partitionBy(
    "trackable_object", "match_id").orderBy("frame").rangeBetween(0, 3000)

# Calculate the sum of distance in 5-minute windows for each player
df = df.withColumn("5min_sum_distance", F.sum(
    "distance").over(five_minute_window))

# Show the result only selected columns
df.select("match_id", "frame", "trackable_object",
          "timestamp", "x", "y", "5min_sum_distance").show()


# Find the initial and final frames of the most intense 5-minute interval for each player
result_df = df.withColumn("max_distance", F.max("5min_sum_distance").over(
    Window.partitionBy("trackable_object", "match_id")))

result_df = result_df.filter(
    F.col("5min_sum_distance") == F.col("max_distance"))

result_df = (result_df.select("match_id", "trackable_object", "frame", "timestamp", "max_distance")
             .withColumn("final_frame", F.col("frame") + 3000)
             .withColumn("final_timestamp", F.date_format(F.col("timestamp")+F.expr("INTERVAL 300 SECONDS"), "HH:mm:ss.SS"))
             .orderBy(["match_id", "trackable_object"]))


# Show the result
result_df.show()