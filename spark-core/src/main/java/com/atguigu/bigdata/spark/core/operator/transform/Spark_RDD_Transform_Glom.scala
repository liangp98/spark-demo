package com.atguigu.bigdata.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark_RDD_Transform_Glom {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local[*]").setAppName("RDD");

    val sc = new SparkContext(sparkConf);

    val data = List(1,2,3,4)

    val rdd: RDD[Int] = sc.makeRDD(data, 2)


    /**
     * 将元素包装成相同类型的集合
     */
    val value: RDD[Array[Int]] = rdd.glom()

    val value1: RDD[Int] = value.map(a => {
      a.max
    })

    println(value1.collect().sum)


    sc.stop()

  }
}
