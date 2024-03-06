package com.atguigu.bigdata.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}


object Spark_RDD_Transform_MapPartitions {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local[*]").setAppName("RDD");

    val sc = new SparkContext(sparkConf);

    val rdd = sc.makeRDD(List(1, 2, 3, 4),2)

    //返回每个分区的最大值
    val rdd2 = rdd.mapPartitions(
      iter => {
        List(iter.max).iterator
      })

    rdd2.collect().foreach(println)
    sc.stop()

  }
}
