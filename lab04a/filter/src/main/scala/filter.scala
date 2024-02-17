import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.net.{URL, URLDecoder}
import scala.util.Try

object filter {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("arseniy_ahtaryanov_lab04")
      .getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    val param_topic_name: String = spark.sparkContext.getConf.get("spark.filter.topic_name")
    val param_offset: String = spark.sparkContext.getConf.get("spark.filter.offset")
    val param_prefix: String = spark.sparkContext.getConf.get("spark.filter.output_dir_prefix")

    val kafka_topic: DataFrame = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "spark-master-1:6667")
      .option("subscribePattern", s"$param_topic_name")
      .option("startingOffsets",
      if(param_offset.contains("earliest"))
        param_offset
      else {
        "{\"" + param_topic_name + "\":{\"0\":" + param_offset + "}}" })
      .load()

    val kafka_logs = kafka_topic.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    val schema = new StructType()
      .add("event_type", StringType, true)
      .add("category", StringType, true)
      .add("item_id", StringType, true)
      .add("item_price", IntegerType, true)
      .add("uid", StringType, true)
      .add("timestamp", LongType, true)

    val df_schema = kafka_logs.withColumn("val", from_json(col("value"), schema))

    val df = df_schema.select(col("val.event_type"), col("val.category"),
        col("val.item_id"), col("val.item_price"),
        col("val.timestamp"), col("val.uid"))
        .withColumn("date", date_format((col("timestamp")/1000).cast("timestamp"), "YYYYMMDD"))
        .withColumn("p_date", col("date").cast("string"))

    val prefix =
      if((param_prefix.startsWith("file:/")) || param_prefix.startsWith("hdfs:/"))
        s"$param_prefix"
      else {
        s"file:///user/arseniy.ahtaryanov/$param_prefix"
      }

    //val new_prefix = "file:///tmp//logs/sb1laba04/arseniy.ahtaryanov/visits-offset"
    df.filter(col("date") < 20200429 && col("event_type") === "view").write
      .format("json")
      .partitionBy("p_date")
      .option("path", s"$prefix/view")
      // hdfs:///user/arseniy.ahtaryanov/visits/view
      // $prefix/view
      .mode("overwrite")
      .save()

    df.filter(col("date") < 20200429 && col("event_type") === "buy").write
      .format("json")
      .partitionBy("p_date")
      .option("path", s"$prefix/buy")
      // hdfs:///user/arseniy.ahtaryanov/visits/buy
      // $prefix/buy
      .mode("overwrite")
      .save()
    // spark-submit --master local[1] --conf spark.filter.topic_name=lab04_input_data --conf spark.filter.offset=earliest --conf spark.filter.output_dir_prefix=hdfs:///user/arseniy.ahtaryanov/visits --class filter --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 ./target/scala-2.11/filter_2.11-1.0.jar
  }
}