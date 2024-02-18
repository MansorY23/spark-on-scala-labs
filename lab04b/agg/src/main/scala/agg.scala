import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.ProcessingTime

import java.net.{URL, URLDecoder}
import scala.util.Try

object agg {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("lab04_aaa")
    .getOrCreate()
  spark.conf.set("spark.sql.session.timeZone", "UTC")
  //spark.conf.set("spark.cleaner.referenceTracking.cleanCheckpoints", "true")

  val kafka_topic: DataFrame = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "spark-master-1:6667")
    .option("subscribePattern", "arseniy_ahtaryanov")
    .option("startingOffsets", "earliest")
    .load()
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

  val schema = new StructType()
    .add("event_type", StringType, true)
    .add("category", StringType, true)
    .add("item_id", StringType, true)
    .add("item_price", IntegerType, true)
    .add("uid", StringType, true)
    .add("timestamp", LongType, true)
  val df_schema = kafka_topic.withColumn("val", from_json(col("value"), schema))

  val df = df_schema.select(col("val.event_type"), col("val.category"),
    col("val.item_id"),  col("val.item_price"),
    to_timestamp(col("val.timestamp")/1000).as("start_ts"),
    //(to_timestamp(col("val.timestamp")/1000) + expr("INTERVAL 1 HOURS")).as("end_ts"),
    col("val.uid"))

  val ready_df = df.select(col("start_ts"), col("uid"),
      when(col("event_type") === "buy", col("item_price")).as("item_price"),
      when(col("event_type") === "buy", col("event_type")).as("buy"))
    //.withWatermark("start_ts", "1 hours")
    .groupBy(window(col("start_ts"), "1 hours").as("time"))
    .agg(sum("item_price").as("revenue"),
      count("uid").as("visitors"),
      count("buy").as("purchases"),
      (sum("item_price")/count("buy")).as("aov"))

  val select_df = ready_df.select(unix_timestamp(col("time.start")).as("start_ts"),
    unix_timestamp(col("time.end")).as("end_ts"),
    col("revenue"), col("visitors"),
    col("purchases"), col("aov"))

  val output_stream =
    select_df
      .select(lit(""), to_json(struct(col("*")))).toDF("key", "value")
      .selectExpr("key", "value")
      .writeStream
      .trigger(ProcessingTime("5 seconds"))
      .format("kafka")
      .option("kafka.bootstrap.servers", "spark-master-1:6667")
      .option("checkpointLocation", "checkpoint")
      .option("topic", "arseniy_ahtaryanov_lab04b_out")
      .outputMode("update")
      .start()
  output_stream.awaitTermination()
}