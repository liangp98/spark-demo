package com.atguigu.bigdata.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark_RDD_Transform_Coalesce {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local[*]").setAppName("RDD");

    val sc = new SparkContext(sparkConf);

    val rdd = sc.makeRDD(List(1,2,3,4,5,6),3)

    /**
     * 分区缩减
     *
     * 若扩大分区，shuffle=true
     */
    val cDdd: RDD[Int] = rdd.coalesce(2,true)

    cDdd.saveAsTextFile("output");
    sc.stop()

  }
}
