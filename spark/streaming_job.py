from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, lit, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"

INPUT_TOPICS = ",".join([
    "patient.heartrate",
    "patient.temperature",
    "patient.oxygenlevel",
    "patient.bloodpressure",
    "patient.respiratoryrate",
])

WARNING_TOPIC = "patient.alerts.warning"
CRITICAL_TOPIC = "patient.alerts.critical"

MONGO_SENSOR_URI = "mongodb://root:root@mongodb:27017/patient_monitoring.sensor_readings?authSource=admin"
MONGO_ALERT_URI = "mongodb://root:root@mongodb:27017/patient_monitoring.alerts?authSource=admin"

schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("patientId", StringType(), True),
    StructField("deviceId", StringType(), True),
    StructField("metric", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("unit", StringType(), True),
])


spark = (
    SparkSession.builder
    .appName("IoT Patient Monitoring Spark Streaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", INPUT_TOPICS)
    .option("startingOffsets", "latest")
    .load()
)

json_stream = raw_stream.selectExpr("CAST(value AS STRING) AS json_value")

readings = (
    json_stream
    .select(from_json(col("json_value"), schema).alias("data"))
    .select("data.*")
)

classified_readings = (
    readings
    .withColumn(
        "alertLevel",
        when(
            (col("metric") == "HeartRate") &
            ((col("value") < 40) | (col("value") > 150)),
            "critical"
        )
        .when(
            (col("metric") == "HeartRate") &
            ((col("value") < 50) | (col("value") > 120)),
            "warning"
        )
        .when(
            (col("metric") == "Temperature") &
            ((col("value") > 39.5) | (col("value") < 35)),
            "critical"
        )
        .when(
            (col("metric") == "Temperature") &
            (col("value") > 38.0),
            "warning"
        )
        .when(
            (col("metric") == "OxygenLevel") &
            (col("value") < 90),
            "critical"
        )
        .when(
            (col("metric") == "OxygenLevel") &
            (col("value") < 93),
            "warning"
        )
        .when(
            (col("metric") == "BloodPressure") &
            ((col("value") > 180) | (col("value") < 70)),
            "critical"
        )
        .when(
            (col("metric") == "BloodPressure") &
            (col("value") > 130),
            "warning"
        )
        .when(
            (col("metric") == "RespiratoryRate") &
            ((col("value") < 8) | (col("value") > 30)),
            "critical"
        )
        .when(
            (col("metric") == "RespiratoryRate") &
            ((col("value") < 10) | (col("value") > 24)),
            "warning"
        )
        .otherwise("normal")
    )
)


alerts = (
    classified_readings
    .filter(col("alertLevel").isin("warning", "critical"))
    .withColumn(
        "message",
        when(
            col("alertLevel") == "critical",
            lit("CRITICAL alert detected")
        ).otherwise(lit("WARNING alert detected"))
    )
)

alerts_to_kafka = (
    alerts
    .withColumn(
        "topic",
        when(col("alertLevel") == "critical", lit(CRITICAL_TOPIC))
        .otherwise(lit(WARNING_TOPIC))
    )
    .select(
        col("topic"),
        to_json(
            struct(
                col("timestamp"),
                col("patientId"),
                col("deviceId"),
                col("metric"),
                col("value"),
                col("unit"),
                col("alertLevel"),
                col("message")
            )
        ).alias("value")
    )
)


def write_sensor_readings_to_mongo(batch_df, batch_id):
    if batch_df.count() > 0:
        (
            batch_df.write
            .format("mongodb")
            .mode("append")
            .option("connection.uri", MONGO_SENSOR_URI)
            .save()
        )
        print(f"Batch {batch_id}: sensor readings written to MongoDB")


def write_alerts_to_mongo(batch_df, batch_id):
    if batch_df.count() > 0:
        (
            batch_df.write
            .format("mongodb")
            .mode("append")
            .option("connection.uri", MONGO_ALERT_URI)
            .save()
        )
        print(f"Batch {batch_id}: alerts written to MongoDB")


console_query = (
    classified_readings.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", "/tmp/spark-checkpoints/console")
    .start()
)

mongo_sensor_query = (
    classified_readings.writeStream
    .foreachBatch(write_sensor_readings_to_mongo)
    .option("checkpointLocation", "/tmp/spark-checkpoints/mongo-sensor-readings")
    .start()
)

mongo_alert_query = (
    alerts.writeStream
    .foreachBatch(write_alerts_to_mongo)
    .option("checkpointLocation", "/tmp/spark-checkpoints/mongo-alerts")
    .start()
)

kafka_alert_query = (
    alerts_to_kafka.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("checkpointLocation", "/tmp/spark-checkpoints/kafka-alerts")
    .start()
)

print("Spark Streaming job started.")
print("Reading Kafka topics, classifying readings, writing to MongoDB and alert topics...")

spark.streams.awaitAnyTermination()