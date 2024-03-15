package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties

object SparkSQL_JDBC_Mysql_test {
  def main(args: Array[String]): Unit = {
    //创建SparkSQL环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate() //session变量的名字  import xx.implicits._
    import spark.implicits._
    val properties = new Properties()
    properties.put("driver", "com.mysql.cj.jdbc.Driver")
    properties.put("user", "root")
    properties.put("password", "123456")
//    val frame: DataFrame = spark.read.jdbc("jdbc:mysql://127.0.0.1:3306/my_test?serverTimezone=UTC", "teacher", properties)
//    frame.show()
//    frame.write.jdbc("jdbc:mysql://127.0.0.1:3306/my_test?serverTimezone=UTC", "teacher_bak", properties)

//    val fm: RDD[Int] = spark.sparkContext.makeRDD(List(1, 2, 3, 4))
    val fm: RDD[Int] = spark.sparkContext.makeRDD(List(5,6,7,8))
    val frame: DataFrame = fm.toDF("id")
    frame.write.mode(SaveMode.Append).jdbc("jdbc:mysql://127.0.0.1:3306/my_test?serverTimezone=UTC", "test", properties)





    //关闭连接
    spark.close()
  }



}
