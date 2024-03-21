package com.atguigu.bigdata.spark.demo

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

object Spark_ReplaceP_Demo {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("staff_login_log")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    var tableName  ="dwd_staff_action_log_8350000_202303_bak"
    val tableActionData: DataFrame = getDataFrameByTableName(spark, tableName)

    //员工动作表
    tableActionData.createOrReplaceTempView("dwd_staff_action_log_8350000_202303")
    val frame: DataFrame = spark.sql("select * from dwd_staff_action_log_8350000_202303 where staff_id='1130000'")
    frame.show()

  }


  def getDataFrameByTableName(spark:SparkSession,tableName:String) :DataFrame = {
    val properties = new Properties()
    properties.put("driver", "org.postgresql.Driver")
    properties.put("user", "xy10000jy_test")
    properties.put("password", "FF10000_20220623#pg")
    val frame: DataFrame = spark.read.jdbc("jdbc:postgresql://10.11.1.101:16601/fj10000jy?currentSchema=jy_report&useUnicode=true&characterEncoding=utf8&useSSL=false", tableName, properties)
    frame
  }


}
