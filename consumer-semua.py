from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from prettytable import PrettyTable

schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("suhu", FloatType(), True),
    StructField("lokasi", StringType(), True),
    StructField("timestamp", StringType(), True)
])

def format_output(rows):
    table = PrettyTable()
    table.field_names = ["Sensor ID", "Suhu (°C)", "Lokasi", "Timestamp", "Status"]

    for row in rows:
        status = "PERINGATAN: Suhu tinggi!" if row['suhu'] > 80 else "Normal"
        table.add_row([row['sensor_id'], row['suhu'], row['lokasi'], row['timestamp'], status])

    print(table)

def consume_and_process_data():
    spark = SparkSession.builder \
        .appName("KafkaSensorConsumer") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "sensor-suhu") \
        .option("startingOffsets", "latest") \
        .load()
    
    df = df.selectExpr("CAST(value AS STRING) as json_data")
    
    json_df = df.select(from_json(col("json_data"), schema).alias("data")).select("data.*")

    def process_batch(batch_df, batch_id):
        rows = batch_df.collect()
        format_output([row.asDict() for row in rows])

    query = json_df.writeStream \
        .outputMode("append") \
        .foreachBatch(process_batch) \
        .start()
    
    query.awaitTermination()

if __name__ == "__main__":
    consume_and_process_data()
