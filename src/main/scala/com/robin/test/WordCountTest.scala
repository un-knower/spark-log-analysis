package com.robin.test

import org.apache.spark.{SparkConf, SparkContext}

object WordCountTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]").setAppName("WordCountTest")
    val sc = new SparkContext(conf)
    val input = sc.textFile("D:\\workspace\\spark-log-analysis\\src\\main\\scala\\com\\robin\\data\\NOTICE.txt")
    val wordCount = input.
      flatMap(line => line.split(" ")).
      map(word=>(word,1)).reduceByKey((x,y)=>(x+y))
    wordCount.foreach(println)
    sc.stop()
  }
}
