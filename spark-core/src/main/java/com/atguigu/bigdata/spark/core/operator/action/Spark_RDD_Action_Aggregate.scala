package com.atguigu.bigdata.spark.core.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_RDD_Action_Aggregate {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("action")

    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    /**
     *  aggregateByKey:  初始值zeroValue只会参与分区内的计算     结果是30
     *  aggregate:  初始值zeroValue同时参与分区内和分区间的计算    结果是40
     */
    val int: Int = rdd.aggregate(10)(_ + _, _ + _)  //结果是40

    println(int)

    sc.stop()

  }

}
