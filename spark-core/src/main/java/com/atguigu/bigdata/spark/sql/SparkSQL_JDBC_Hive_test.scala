package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties

object SparkSQL_JDBC_Hive_test {
  def main(args: Array[String]): Unit = {
    //创建SparkSQL环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate() //session变量的名字  import xx.implicits._
    import spark.implicits._
//    System.setProperty("HADOOP_USER_NAME","appuser");
    spark.sql("use spark_db").show()
    println()
    spark.sql(
      """CREATE TABLE IF NOT exists `user_visit_action`(
        | `date` string,
        | `user_id` bigint,
        | `session_id` string,
        | `page_id` bigint,
        | `action_time` string,
        | `search_keyword` string,
        | `click_category_id` bigint,
        | `click_product_id` bigint,
        | `order_category_ids` string,
        | `order_product_ids` string,
        | `pay_category_ids` string,
        | `pay_product_ids` string,
        | `city_id` bigint)
        |row format delimited fields terminated by '\t'""".stripMargin)

    spark.sql("""load data local inpath 'datas/user_visit_action.txt' into table spark_db.user_visit_action""".stripMargin)

    spark.sql("""CREATE TABLE `product_info`(
                | `product_id` bigint,
                | `product_name` string,
                | `extend_info` string)
                |row format delimited fields terminated by '\t'
                |""".stripMargin)
    spark.sql("""load data local inpath 'datas/product_info.txt' into table spark_db.product_info""")

    spark.sql("""CREATE TABLE `city_info`(
                | `city_id` bigint,
                | `city_name` string,
                | `area` string)
                |row format delimited fields terminated by '\t'
                |""".stripMargin)

    spark.sql("""load data local inpath 'datas/city_info.txt' into table spark_db.city_info
                |""".stripMargin)




    //关闭连接
    spark.close()
  }


}
