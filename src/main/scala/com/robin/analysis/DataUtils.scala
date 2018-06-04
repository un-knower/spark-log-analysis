package com.robin.analysis

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat



/**
  * 日期时间解析工具类
  * 注意:SimpleDateFormat是线程不安全的
  */
object DataUtils {
  // [10/Nov/2016:00:01:02+0800] ===》 yyyy-MM-dd HH:mm:ss
  // 输入文件日期时间格式
  val YYYYMMMMDDHHMM_TIME_FORMAT = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ssZ",Locale.ENGLISH)

  //目标日期格式
  val TRAGET_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")


  /**
    * 获取时间: yyyy-MM-dd HH:mm:ss
    * @param time
    */
  def parse (time:String): String={
    TRAGET_FORMAT.format(new Date(getTime(time)))
  }

  /**
    * 获取输入日志时间: long类型
    * @param time
    * @return 返回截取好的字段类型
    */
  def getTime(time:String): Long ={
    try{
      YYYYMMMMDDHHMM_TIME_FORMAT.parse(time.substring(time.indexOf("[")+1,
        time.lastIndexOf("]"))).getTime
    }catch {
      case  e: Exception => {
        0l
      }
    }
  }


  def main(args: Array[String]): Unit = {
    println(parse("[10/Nov/2016:00:01:02+0800]"))
  }

}
