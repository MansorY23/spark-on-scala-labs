ThisBuild / version := "1.0"

ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "data_mart"
  )

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.4.7" % "provided"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.4.7" % "provided"
libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector_2.11" % "2.4.3" % "provided"
libraryDependencies += "org.elasticsearch" % "elasticsearch-spark-20_2.11" % "6.8.2" % "provided"
libraryDependencies += "org.postgresql" % "postgresql" % "42.3.3" % "provided"
