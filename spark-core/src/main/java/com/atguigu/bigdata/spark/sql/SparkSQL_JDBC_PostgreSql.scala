package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties

object SparkSQL_JDBC_PostgreSql {
  def main(args: Array[String]): Unit = {
    //创建SparkSQL环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate() //session变量的名字  import xx.implicits._
    val properties = new Properties()
    properties.put("driver", "org.postgresql.Driver")
    properties.put("user", "xy10000jy_test")
    properties.put("password", "FF10000_20220623#pg")
//    properties.put("user", "postgres")
//    properties.put("password", "123456")
//    val frame: DataFrame = spark.read.jdbc("jdbc:postgresql://localhost:5432/test?currentSchema=public", "ts_user", properties)
    val frame: DataFrame = spark.read.jdbc("jdbc:postgresql://10.11.1.101:16601/fj10000jy?currentSchema=multidms&useUnicode=true&characterEncoding=utf8&useSSL=false", "ts_collect_report", properties)

    frame.show()
    frame.write.jdbc("jdbc:postgresql://10.11.1.101:16601/fj10000jy?currentSchema=multidms&useUnicode=true&characterEncoding=utf8&useSSL=false", "ts_collect_report_tttttttt", properties)





    //关闭连接
    spark.close()
  }


}
