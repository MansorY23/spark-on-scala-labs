import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.broadcast
import java.net.{URL, URLDecoder}
import scala.util.Try
import sys.process._

object domains {
  def main(args: Array[String]): Unit = {
    var autousersPath = "/labs/laba02/autousers.json"
    var logs_path = "/labs/laba02/logs"

    val spark = SparkSession.builder()
      .appName("lab02")
      .getOrCreate()
    val logsSchema =

      StructType(
        List(
          StructField("uid", StringType),
          StructField("ts", StringType),
          StructField("url", StringType)
        )
      )
    val domains = spark.read.schema(logsSchema).option("delimiter", "\t").csv(logs_path)

    var autousers = spark.read.json(autousersPath)
    val df_ausers = autousers.select(explode(col("autousers")))

    def decodeUrlAndGetDomain: UserDefinedFunction = udf((url: String) => {
      Try {
        new URL(URLDecoder.decode(url, "UTF-8")).getHost
      }.getOrElse("")
    })

    val domains_users = domains.select(col("uid"),
        decodeUrlAndGetDomain(col("url"))
          .alias("domain"))
      .filter("domain != ''")

    val all_logs = domains_users
      .select(col("uid"),
        expr("ltrim('www.', ltrim(domain))")
          .alias("domain"))
    val total_hits = all_logs.count().toFloat
    val auto_hits = all_logs.join(broadcast(df_ausers),
      df_ausers("col") === domains_users("uid"),
      "leftouter")
    val autousers = auto_hits.select("col").filter(col("col").isNotNull).count().toFloat

    val otaya_df = auto_hits.
      groupBy(col("domain")).
      agg(count(col("col")).as("sum"),
        count(col("uid")).as("count")).withColumn("relevance",
        format_number(
          pow((col("sum") / total_hits), 2) /
            ((col("count") / total_hits) *  (autousers / total_hits)), 15))
      .sort(col("relevance").desc, col("domain").asc).limit(200)

    val final_df = otaya_df.select("domain", "relevance")
    final_df.show()

    val result_path = "lab02_result"
    final_df.coalesce(1).write.mode("overwrite").option("delimiter", "\t").csv(result_path)

    """hdfs dfs -get lab02_result/part-* laba02_domains.txt""".!!
  }
}