from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "users_created"

CASSANDRA_HOST = "cassandra"
CASSANDRA_PORT = "9042"

CASSANDRA_KEYSPACE = "spark_streams"
CASSANDRA_TABLE = "created_users"


# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("RealtimeUserStreaming")
    .master("spark://spark-master:7077")
    .config(
        "spark.cassandra.connection.host",
        CASSANDRA_HOST
    )
    .config(
        "spark.cassandra.connection.port",
        CASSANDRA_PORT
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# KAFKA MESSAGE SCHEMA
# ============================================================

user_schema = StructType([
    StructField("id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("address", StringType(), True),
    StructField("post_code", StringType(), True),
    StructField("email", StringType(), True),
    StructField("username", StringType(), True),
    StructField("dob", StringType(), True),
    StructField("registered_date", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("picture", StringType(), True)
])


# ============================================================
# READ FROM KAFKA
# ============================================================

print("Connecting to Kafka...")

kafka_stream = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP_SERVERS
    )
    .option(
        "subscribe",
        KAFKA_TOPIC
    )
    .option(
        "startingOffsets",
        "earliest"
    )
    .load()
)


# ============================================================
# CONVERT KAFKA BINARY VALUE TO JSON
# ============================================================

users = (
    kafka_stream
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(
        from_json(
            col("json_value"),
            user_schema
        ).alias("data")
    )
    .select("data.*")
)


# ============================================================
# WRITE EACH MICRO-BATCH TO CASSANDRA
# ============================================================

def write_to_cassandra(batch_df, batch_id):

    print(f"Processing Spark batch: {batch_id}")

    if batch_df.isEmpty():
        print("Batch is empty")
        return

    print(f"Records in batch: {batch_df.count()}")

    (
        batch_df.write
        .format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(
            keyspace=CASSANDRA_KEYSPACE,
            table=CASSANDRA_TABLE
        )
        .save()
    )

    print(f"Batch {batch_id} written to Cassandra")


# ============================================================
# START STREAMING
# ============================================================

query = (
    users.writeStream
    .foreachBatch(write_to_cassandra)
    .outputMode("append")
    .option(
        "checkpointLocation",
        "/opt/spark/work-dir/checkpoints/users_created"
    )
    .start()
)


print("==============================================")
print("Realtime User Streaming Started")
print(f"Kafka Topic : {KAFKA_TOPIC}")
print(f"Kafka       : {KAFKA_BOOTSTRAP_SERVERS}")
print(f"Cassandra   : {CASSANDRA_HOST}:{CASSANDRA_PORT}")
print(
    f"Table       : "
    f"{CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE}"
)
print("==============================================")


query.awaitTermination()