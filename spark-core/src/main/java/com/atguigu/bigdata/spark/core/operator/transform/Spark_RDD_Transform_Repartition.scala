package com.atguigu.bigdata.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark_RDD_Transform_Repartition {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local[*]").setAppName("RDD");

    val sc = new SparkContext(sparkConf);

    val rdd = sc.makeRDD(List(1,2,3,4,5,6),2)
    /**
     * 分区扩展
     *
     * 等同于 coalesce(3,true)
     */
    val repartitionDdd: RDD[Int] = rdd.repartition(3)

    repartitionDdd.saveAsTextFile("output");
    sc.stop()

  }
}
