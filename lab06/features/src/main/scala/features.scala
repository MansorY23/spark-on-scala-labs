import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import scala.util.Try
import java.net.{URL, URLDecoder}


object features {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .appName("lab06_aaa")
      .getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    val weblogs = spark
      .read
      .json("/labs/laba03/weblogs.json")

    val users_items = spark
      .read
      .parquet("/user/arseniy.ahtaryanov/users-items/20200429")

    val popular_domains = weblogs
      .select(explode(col("visits.url")).as("url"))
      .withColumn("host", lower(callUDF("parse_url", $"url", lit("HOST"))))
      .withColumn("domain", regexp_replace($"host", "www.", ""))
      .filter(col("domain") =!= "null")
      .groupBy("domain")
      .count()
      .orderBy(col("count").desc)
      .limit(1000)
      .select("domain")
      .orderBy(col("domain").asc)

    val all_user_domains = weblogs
      .select(col("uid"), explode(col("visits.url")).as("url"))
      .withColumn("host", lower(callUDF("parse_url", $"url", lit("HOST"))))
      .withColumn("domain", regexp_replace($"host", "www.", ""))
      .filter(col("domain") =!= "null")
      .select("uid", "domain")

    val domain_features = popular_domains
      .join(all_user_domains, popular_domains("domain") === all_user_domains("domain"), "left")
      .drop(all_user_domains("domain"))
      .groupBy("uid")
      .pivot("domain")
      .count()
      .na.fill(0)
      .withColumn("domains", array(col("*")))
      .select(col("uid"), expr("slice(domains, 2, size(domains))").as("domain_features").cast("array<int>"))

    val user_hours =
      weblogs
        .select(col("uid"), explode(col("visits.timestamp")).as("timestamp"))
        .select(col("uid"), (col("timestamp")/1000).cast("timestamp").as("timestamp"))
        .withColumn("hour", concat(lit("web_hour_"), hour(col("timestamp"))))
        .groupBy("uid")
        .pivot("hour")
        .count()
        .na.fill(0)
    //select(col("uid"), cols("").cast("float"))

    val user_fraction =
      weblogs
        .select(col("uid"), explode(col("visits.timestamp")).as("timestamp"))
        .select(col("uid"), (col("timestamp")/1000).cast("timestamp").as("timestamp"))
        .withColumn("hour", hour(col("timestamp")))
        .withColumn("web_fraction_work_hours",
          when(col("hour") >= 9 && col("hour") < 18, 1))
        .withColumn("web_fraction_evening_hours",
          when(col("hour") >= 18 && col("hour") < 24, 1))
        .groupBy("uid")
        .agg(count("web_fraction_work_hours").as("web_fraction_work_hours"),
          count("web_fraction_evening_hours").as("web_fraction_evening_hours"),
          count("hour").as("all_visits"))
        .select(col("uid"),
          (col("web_fraction_work_hours") / col("all_visits"))
            .as("web_fraction_work_hours"),
          (col("web_fraction_evening_hours") / col("all_visits"))
            .as("web_fraction_evening_hours")
        )

    val user_days =
      weblogs
        .select(col("uid"), explode(col("visits.timestamp")).as("timestamp"))
        .withColumn("day_of_week",  concat(lit("web_day_"), lower(date_format((col("timestamp")/1000).cast("timestamp"), "E" ))))
        .groupBy("uid")
        .pivot("day_of_week")
        .count()
        .na.fill(0)

    val final_df =
      domain_features
        .join(user_hours, "uid")
        .join(user_days, "uid")
        .join(user_fraction, "uid")
        .join(users_items, "uid")

    final_df
      .write
      .mode("overwrite")
      .format("parquet")
      .save("/user/arseniy.ahtaryanov/features")
  }
}