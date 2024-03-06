package com.atguigu.bigdata.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark_RDD_Transform_Filter {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local[*]").setAppName("RDD");

    val sc = new SparkContext(sparkConf);

    val rdd = sc.makeRDD(List(1,2,3,4,5,6,7))

    /**
     * 保留偶数
     */

    val filterRDD: RDD[Int] = rdd.filter(_% 2 == 0)
    filterRDD.collect().foreach(println);

    sc.stop()

  }
}
