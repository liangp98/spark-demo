package com.atguigu.bigdata.spark.core.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_RDD_Action_Take {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("action")

    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //take
    val ints: Array[Int] = rdd.take(3)
    println(ints.mkString(","))


    val rdd1: RDD[Int] = sc.makeRDD(List(4, 2, 1, 3))

    //takeOrdered 正序
    val ints1: Array[Int] = rdd1.takeOrdered(3)
    println(ints1.mkString(","))

    val rdd2: RDD[Int] = sc.makeRDD(List(4, 2, 1, 3))

    //takeOrdered 倒序
    val ints2: Array[Int] = rdd2.takeOrdered(3)(Ordering.Int.reverse)
    println(ints2.mkString(","))

    sc.stop()

  }

}
