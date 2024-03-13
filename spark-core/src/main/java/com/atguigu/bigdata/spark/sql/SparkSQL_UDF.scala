package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * 自定义函数
 */
object SparkSQL_UDF {
  def main(args: Array[String]): Unit = {
    //创建SparkSQL环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._ //session变量的名字  import xx.implicits._

    //自定义函数实现添加 name字段添加前缀功能
    spark.udf.register("prefixName",(username:String)=>{
      "Name:" + username
    })


    val df: DataFrame = spark.read.json("datas/user.json")
    df.createOrReplaceTempView("user")
    spark.sql("select age,prefixName(username) from user").show()


    //关闭连接
    spark.close()


  }


}
