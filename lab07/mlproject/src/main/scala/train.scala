import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{CountVectorizer, IndexToString, StringIndexer}
import org.apache.spark.ml.{Pipeline, PipelineModel}

class train {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("lab07_aaa")
      .getOrCreate()  }

  val weblogs_path: String = spark.sparkContext.getConf.get("/")
  val spark_pipeline: String = spark.sparkContext.getConf.get("/")

  val weblogs = spark
    .read
    .json(weblogs_path)

  val train_dataset  =
    weblogs
      .select(col("uid"), col("gender_age"), explode(col("visits.url")).as("url"))
      .withColumn("host", lower(callUDF("parse_url", $"url", lit("HOST"))))
      .withColumn("domains", regexp_replace($"host", "www.", ""))
      .select(col("uid"), col("gender_age"), col("domains"))
      .groupBy(col("uid"), col("gender_age"))
      .agg(collect_list("domains").as("domains"))

  val cv = new CountVectorizer()
    .setInputCol("domains")
    .setOutputCol("features")

  val indexer = new StringIndexer()
    .setInputCol("gender_age")
    .setOutputCol("label")

  val indexed = indexer
    .fit(train_dataset)

  // математика какаято
  val lr = new LogisticRegression()
    .setMaxIter(10)
    .setRegParam(0.001)

  // созданные индексы модели преобразовать в понятные пол и возраст(категории)
  val converter = new IndexToString()
    .setInputCol("prediction")
    .setOutputCol("original_category")
    .setLabels(indexed.labels)
  //val converted = converter.transform(indexed)

  // создание пайплайна модели
  val pipeline = new Pipeline()
    .setStages(Array(cv, indexer, lr, converter))

  // обучение модели
  val model = pipeline.fit(train_dataset)

  // save model to folder
  model.write.overwrite().save(spark_pipeline)

}