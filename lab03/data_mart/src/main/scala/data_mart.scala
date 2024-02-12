import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

import java.net.{URL, URLDecoder}
import scala.util.Try
object data_mart {

  val spark: SparkSession = SparkSession
    .builder()
    .config("spark.cassandra.connection.host", "10.0.0.31")
    .config("spark.cassandra.connection.port", "9042")
    .getOrCreate()

  val clients: DataFrame = spark.read
    .format("org.apache.spark.sql.cassandra")
    .options(Map("table" -> "clients", "keyspace" -> "labdata"))
    .load()

  val visits: DataFrame = spark.read
    .format("org.elasticsearch.spark.sql")
    .options(Map("es.read.metadata" -> "true",
      "es.nodes.wan.only" -> "true",
      "es.port" -> "9200",
      "es.nodes" -> "10.0.0.31",
      "es.net.ssl" -> "false"))
    .load("visits")

  val logs: DataFrame = spark.read
    .json("hdfs:///labs/laba03/weblogs.json")

  val cats: DataFrame = spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://10.0.0.31:5432/labdata")
    .option("dbtable", "domain_cats")
    .option("user", "arseniy_ahtaryanov")
    .option("password", "qF8g2i1Q")
    .option("driver", "org.postgresql.Driver")
    .load()

  val clients_age_cat = clients
    .withColumn("age_cat",
      when(col("age") >= "18" && col("age") <= "24" , "18-24")
        .when(col("age") >= "25" && col("age") <= "34" , "25-34")
        .when(col("age") >= "35" && col("age") <= "44" , "35-44")
        .when(col("age") >= "45" && col("age") <= "54" , "45-54")
        .otherwise(">=55"))

  def decodeUrlAndGetDomain: UserDefinedFunction = udf((url: String) => {
    Try {
      new URL(URLDecoder.decode(url, "UTF-8")).getHost
    }.getOrElse("")
  })

  val user_urls = logs
    .select(col("uid"), explode(col("visits.url")).as("url"))

  val user_domains = user_urls
    .select(col("uid"), decodeUrlAndGetDomain(col("url")).as("domain"))
    .filter("url != ''")

  val user_visits = user_domains
    .select(col("uid"), expr("ltrim('www.', ltrim(domain))").as("domain"))

  val web_visit_cat = user_visits
    .join(broadcast(cats), "domain")
    .select(col("uid"), col("domain"),
      concat(lit("web_"), col("category")).as("category"))

  val web_visits = web_visit_cat
    .groupBy("uid")
    .pivot("category")
    .agg(count("uid"))
    .na.fill(0)

  val shop_visits_cat = visits
    .select(regexp_replace(
      concat(lit("shop_"), lower(col("category")))
      , lit('-'), lit('_'))
      .as("category"), col("uid"))
    .na.drop("any")

  val shop_visits = shop_visits_cat
    .groupBy("uid")
    .pivot("category")
    .agg(count("uid"))
    .na.fill(0)

  val clients_final = clients_age_cat.select(col("uid"), col("gender"), col("age_cat"))

  val result = clients_final
    .join(shop_visits, "uid")
    .join(web_visits, "uid")

  result.write
    .format("jdbc")
    .option("url", "jdbc:postgresql://10.0.0.31:5432/arseniy_ahtaryanov") //как в логине к личному кабинету но _ вместо .
    .option("dbtable", "clients")
    .option("user", "arseniy_ahtaryanov")
    .option("password", "qF8g2i1Q")
    .option("driver", "org.postgresql.Driver")
    .option("truncate", value = true) //позволит не терять гранты на таблицу
    .mode("overwrite") //очищает данные в таблице перед записью
    .save()
}