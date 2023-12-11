from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("example").getOrCreate()

# Read data from Parquet file using the absolute path
file_path = "/mounted-data/data/processed/trackable_object.parquet"
frame_path = "/mounted-data/data/processed/timeframe.parquet"

# Load trackable_object data
df = spark.read.parquet(file_path) # 4.985.145
df_time = spark.read.parquet(frame_path)

# Create a temporary view
df.createOrReplaceTempView("trackable_object_view")


consecutive_max_distance_query = \
"""
WITH windowed_distance AS (
    SELECT
        match_id,
        FLOOR(frame / 1200) as window_id,
        SUM(distance) as total_distance,
        MIN(frame) as initial_frame,
        MAX(frame) as final_frame,
        ROW_NUMBER() OVER (PARTITION BY match_id, FLOOR(frame / 1200) ORDER BY SUM(distance) DESC) as rank
    FROM (
        SELECT
            a.match_id,
            a.frame,
            SQRT(POW(a.x - b.x, 2) + POW(a.y - b.y, 2)) as distance
        FROM
            trackable_object_view a
        JOIN
            trackable_object_view b
        ON
            a.match_id = b.match_id
            AND a.frame = b.frame
        WHERE
            a.trackable_object < b.trackable_object
    )
    GROUP BY
        match_id, window_id
)

SELECT
    match_id,
    window_id,
    max_distance,
    initial_frame,
    final_frame
FROM (
    SELECT
        match_id,
        window_id,
        MAX(total_distance) as max_distance,
        MIN(initial_frame) as initial_frame,
        MAX(final_frame) as final_frame,
        ROW_NUMBER() OVER (PARTITION BY match_id ORDER BY MAX(total_distance) DESC) as rank
    FROM windowed_distance
    GROUP BY match_id, window_id
) ranked
WHERE rank = 1
ORDER BY match_id, window_id;
"""

consecutive_max_distance_df = spark.sql(consecutive_max_distance_query)

# Join dataframes to add timestamp
consecutive_max_distance_df = consecutive_max_distance_df.join(
    df_time.selectExpr("match_id as match_id", "frame as frame", "timestamp as timestamp"),
    (consecutive_max_distance_df.match_id == df_time.match_id) & (consecutive_max_distance_df.initial_frame == df_time.frame),
    how="left"
).drop("match_id_time", "frame_time")


# Select and Show only required columns
consecutive_max_distance_df.select(
    "match_id",
    "window_id",
    "max_distance",
    "initial_frame",
    "final_frame",
    "initial_timestamp",
    (F.col("initial_timestamp") + F.expr("INTERVAL 120 SECONDS")).alias("final_timestamp")
).show()
