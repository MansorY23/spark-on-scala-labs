import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.util.Try

object users_items {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .appName("lab05_aaa")
      .getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    val param_input_dir: String = spark.sparkContext.getConf.get("spark.users_items.input_dir")
    val param_output_dir: String = spark.sparkContext.getConf.get("spark.users_items.output_dir")
    val param_items_update: Int = spark.sparkContext.getConf.get("spark.users_items.update").toInt


    val view_df: DataFrame = spark
      .read
      .json(s"$param_input_dir/view")
      .select(regexp_replace(
        concat(lit("view_"), lower(col("item_id"))), lit('-'), lit('_')
      ).as("item_id"), col("uid"), col("date"))

    val buy_df: DataFrame = spark
      .read
      .json(s"$param_input_dir/buy")
      .select(regexp_replace(
        concat(lit("buy_"), lower(col("item_id"))), lit('-'), lit('_')
      ).as("item_id"), col("uid"), col("date"))

    val union_df = view_df.union(buy_df)

    val matrix = union_df
      .groupBy("uid")
      .pivot("item_id")
      .count()
      .na.fill(0)

    val last_date = union_df.select(max("date"))

    if (param_items_update == 0) {
      matrix
        .write
        .format("parquet")
        .option("path", s"hdfs://$param_output_dir/$last_date")
        .mode("overwrite")
        .save()
    } else {
      val last_matrix = spark
        .read
        .format("parquet")
        .option("path", s"hdfs://$param_output_dir/*")
        .load()

      val final_df = last_matrix.join(matrix, "inner")
      final_df
        .write
        .format("parquet")
        .option("path", s"hdfs://$param_output_dir/$last_date")
        .save()
    }
  }
}
