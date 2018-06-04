name := "spark-log-analysis"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.2.0",
  // https://mvnrepository.com/artifact/org.apache.spark/spark-sql
  "org.apache.spark" % "spark-sql_2.11" % "2.2.0",
  "com.alibaba" % "fastjson" % "1.1.23"
)