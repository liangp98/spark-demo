package com.atguigu.bigdata.spark.core.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_RDD_Action_Foreach {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("action")

    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)


    /**
     *    rdd.foreach(println) 和 rdd.collect().foreach(println)的区别
     *
     *  rdd.collect().foreach(println) 的println是Driver内存中执行的
     *  rdd.foreach(println) 的println是Executor中执行的
     *
     *
     */

    rdd.collect().foreach(println)
    println("----------------------------")
    rdd.foreach(println)


    sc.stop()

  }

}
