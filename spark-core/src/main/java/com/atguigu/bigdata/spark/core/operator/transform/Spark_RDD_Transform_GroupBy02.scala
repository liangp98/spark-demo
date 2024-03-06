package com.atguigu.bigdata.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark_RDD_Transform_GroupBy02 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local[*]").setAppName("RDD");

    val sc = new SparkContext(sparkConf);

    val rdd: RDD[String] = sc.textFile("datas/apache.log")

    /**
     * 计算每个时段的访问次数
     */
    val value: RDD[String] = rdd.map(e => {
      //      17/05/2015:10:05:03
      //      2015:10:05:03
      e.split(" ").apply(3).split(":").apply(1)
    })
    val value1: RDD[(String, Iterable[String])] = value.groupBy(_.toString)
    val value2: RDD[(String, Int)] = value1.map(e => {
      (e._1, e._2.size)
    })

    value2.collect().sorted.foreach(println)

    sc.stop()

  }
}
