package com.atguigu.bigdata.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark_RDD_Transform_Distinct {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local[*]").setAppName("RDD");

    val sc = new SparkContext(sparkConf);

    val rdd = sc.makeRDD(List(1,2,3,4,1,2,3,5))
    /**
     * 去重
     */
    val dRdd: RDD[Int] = rdd.distinct()
    dRdd.collect().foreach(println)

    sc.stop()

  }
}
