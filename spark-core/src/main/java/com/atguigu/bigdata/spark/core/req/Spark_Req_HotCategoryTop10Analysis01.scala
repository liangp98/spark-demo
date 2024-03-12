package com.atguigu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.io;

object Spark_Req_HotCategoryTop10Analysis01 {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("save")

    val sc = new SparkContext(sparkConf)

    //获取数据
    val srcRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")
    //处理点击，下单，支付数据
    val value:RDD[(String,(Int,Int,Int))] = srcRDD.flatMap(
      line => {
        val str: Array[String] = line.split("_")
        if (str(6) != "-1") {
          //点击
          List((str(6), (1, 0, 0)))
        } else if (str(8) != "null") {
          //下单
          val orderStr: Array[String] = str(8).split(",")
          orderStr.map(m => (m, (0, 1, 0)))
        } else if (str(10) != "null") {
          //支付
          val payStr: Array[String] = str(10).split(",")
          payStr.map(m => (m, (0, 0, 1)))
        } else {
          Nil
        }
      }
    )
    val value1: RDD[(String, (Int, Int, Int))] = value.reduceByKey(
      (map1, map2) => {
        (map1._1 + map2._1, map1._2 + map2._2, map1._3 + map2._3)
      }
    )
    //输出结果
    value1.sortBy(_._2,false).take(10).foreach(println)

    sc.stop()

  }


}
