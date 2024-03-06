package com.atguigu.bigdata.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark_RDD_Transform_GroupByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local[*]").setAppName("RDD");

    val sc = new SparkContext(sparkConf);

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("b", 4)))
    /**
     * groupByKey: 将相同key的value进行分组,不做聚合操作
     *
     */
    val groupByKeyRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    groupByKeyRDD.collect().foreach(println)

    println("-------------------------")
    val groupByRDD1: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)
    groupByRDD1.collect().foreach(println)

    sc.stop()

  }
}
