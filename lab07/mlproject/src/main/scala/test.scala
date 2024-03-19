import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.ml.{Pipeline, PipelineModel}

class test {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("lab07_work")
      .getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    val model_path = spark.SparkContext.getConf.get("")
    val input_topic = spark.SparkContext.getConf.get("")
    val output_topic = spark.SparkContext.getConf.get("")
    // checkpoint location
    val checkpoint = "/user/arseniy.ahtaryanov/lab07/checkpoint"

    val kafka_topic: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "spark-master-1:6667")
      .option("subscribePattern", input_topic)
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    val model = PipelineModel.load(model_path)

    val schema = StructType(Array(
      StructField("uid", StringType),
      StructField("visits", ArrayType(StructType(Array(
        StructField("url", StringType),
        StructField("timestamp", LongType)
      ))
      ))))
    val kafka_value = kafka_topic.withColumn("val", from_json(col("value"), schema))

    val test_stream = kafka_value
      .select(col("val.uid").as("uid"), explode(col("val.visits")).as("visits"))
      .withColumn("host", lower(callUDF("parse_url", col("visits.url"), lit("HOST"))))
      .withColumn("domains", regexp_replace(col("host"), "www.", ""))
      .groupBy(col("uid"))
      .agg(collect_list("domains").as("domains"))

    val prediction_stream = model.transform(test_stream)

    val output_stream =
    prediction_stream
      .select(lit(""),
        to_json(struct(col("uid"),
          col("original_category").as("gender_age")))).toDF("key", "value")
      .selectExpr("key", "value")
      .writeStream
      .trigger(ProcessingTime("5 seconds"))
      .format("kafka")
      .option("kafka.bootstrap.servers", "spark-master-1:6667")
      .option("checkpointLocation", checkpoint)
      .option("topic", output_topic)
      .outputMode("update")
      .start()
    output_stream.awaitTermination()
  }

}
