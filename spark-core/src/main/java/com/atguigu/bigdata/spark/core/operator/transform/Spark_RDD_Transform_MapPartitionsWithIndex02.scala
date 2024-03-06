package com.atguigu.bigdata.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}


object Spark_RDD_Transform_MapPartitionsWithIndex02 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local[*]").setAppName("RDD");

    val sc = new SparkContext(sparkConf);

    val rdd = sc.makeRDD(List("A","B","C","D"))

    /**
     * 每个元素所在的分区索引好
     */
    val rdd2 = rdd.mapPartitionsWithIndex(
      (index,iter) => {
        iter.map((index,_))
      })

    rdd2.collect().foreach(println)
    sc.stop()

  }
}
