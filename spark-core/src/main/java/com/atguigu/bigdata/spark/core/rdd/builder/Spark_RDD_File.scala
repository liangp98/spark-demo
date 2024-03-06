package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_RDD_File {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd1: RDD[String] = sc.textFile("datas/1.txt")
    rdd1.collect().foreach(println)
    println("==================================")

    val rdd2: RDD[String] = sc.textFile("datas/1*.txt")
    rdd2.collect().foreach(println)
    println("==================================")

    val rdd3: RDD[String] = sc.textFile("datas")
    rdd3.collect().foreach(println)

    sc.stop()

  }

}
