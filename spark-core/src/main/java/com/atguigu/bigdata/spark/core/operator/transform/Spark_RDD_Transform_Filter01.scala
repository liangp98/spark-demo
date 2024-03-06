package com.atguigu.bigdata.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark_RDD_Transform_Filter01 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local[*]").setAppName("RDD");

    val sc = new SparkContext(sparkConf);
    val rdd: RDD[String] = sc.textFile("datas/apache.log")

    /**
     * 获取 2015.5.17请求路径
     */
    val value: RDD[String] = rdd.filter(e => {
      //      83.149.9.216 - - 17/05/2015:10:05:03
      val arr: Array[String] = e.split(" ")
      arr.apply(3).split(":").apply(0).equals("17/05/2015")
    }).map(e => {
      e.split(" ").apply(6)
    })

    value.collect().foreach(println)


    sc.stop()

  }
}
