from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType

schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("suhu", FloatType(), True),
    StructField("lokasi", StringType(), True),
    StructField("timestamp", StringType(), True)
])

def consume_and_process_data():
    spark = SparkSession.builder \
        .appName("KafkaSensorConsumer") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    # Membaca stream dari Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "sensor-suhu") \
        .option("startingOffsets", "latest") \
        .load()
    
    df = df.selectExpr("CAST(value AS STRING) as json_data")
    
    json_df = df.select(from_json(col("json_data"), schema).alias("data")).select("data.*")
    
    alert_df = json_df.filter(col("suhu") > 80)

    query = alert_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="5 seconds") \
        .start()
    
    query.awaitTermination()

if __name__ == "__main__":
    consume_and_process_data()
