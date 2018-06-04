package com.robin.log

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 使用Spark完成我们的数据清洗操作
  */
object SparkStatCleanJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkStatCleanJob")
      //.config("spark.sql.parquet.compression.codec","gzip")
      .config("spark.testing.memory","2147480000")
      //.master("local[2]")
      .getOrCreate()

    val accessRDD = spark.sparkContext.
      textFile("file:///home/hadoop/data/output")

//    accessRDD_win.take(10).foreach(println)
    //RDD ==> DF
    val accessDF = spark.createDataFrame(accessRDD.map(x => AccessConvertUtil.parseLog(x)),
      AccessConvertUtil.struct)

    accessDF.printSchema()
    accessDF.show(false)

    spark.stop()
  }

}
