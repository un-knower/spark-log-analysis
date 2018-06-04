package com.robin.test

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 测试Spark的UDF 和 UDAF 函数
  */
object UDFTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("UDFTest")

    conf.set("spark.testing.memory", "2147480000")

    val sc = new SparkContext(conf)
    val hiveContext = new SQLContext(sc)

    val bigData = Array("Spark","Hadoop","Flink","Spark","Hadoop","Flink","Spark","Hadoop","Flink","Spark","Hadoop","Flink")
    val bigDataRDD = sc.parallelize(bigData)

    val bigDataRowRDD = bigDataRDD.map(line => Row(line))
    val structType = StructType(Array(StructField("name",StringType,true)))
    val bigDataDF = hiveContext.createDataFrame(bigDataRowRDD, structType)

    bigDataDF.registerTempTable("bigDataTable")

    /**
      * 通过hiveContext注册UDF
      */
    hiveContext.udf.register("computeLength", UDF.computeLength _)
    hiveContext.sql(
      s"""
         |SELECT name
         |      ,computeLength(name) as
         |FROM bigDataTable
       """.stripMargin).show()

    sc.stop()
  }
}
