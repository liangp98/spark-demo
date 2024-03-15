package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties

object SparkSQL_JDBC_Hive {
  def main(args: Array[String]): Unit = {
    //创建SparkSQL环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate() //session变量的名字  import xx.implicits._
    import spark.implicits._
    val properties = new Properties()
    properties.put("driver", "org.apache.hive.jdbc.HiveDriver")
    properties.put("user", "hive")
    properties.put("password", "Aw=5%x7yd6_x")
//    val frame: DataFrame = spark.read.jdbc("jdbc:hive2://10.11.5.140:10000/report", "test", properties)
//    frame.show()
//    frame.write.jdbc("jdbc:hive2://10.11.5.140:10000/report", "test", properties)


    val ids: RDD[String] = spark.sparkContext.makeRDD(List("1","2"))

    val frame: DataFrame = ids.toDF("id")
//    val frame: DataFrame = ids.toDF()

    frame.write.jdbc("jdbc:hive2://10.11.5.140:10000/report", "test", properties)

//    frame.write.format(source = "jdbc")
//      .mode(SaveMode.Append)
//      .option("url", "jdbc:hive2://10.11.5.140:10000/report")
//      .option("dbtable", "wlp")
//      .option("user", "hive")
//      .option("password", "Aw=5%x7yd6_x")
//      .option("driver", "org.apache.hive.jdbc.HiveDriver")
//      .save()


    //关闭连接
    spark.close()
  }


}
