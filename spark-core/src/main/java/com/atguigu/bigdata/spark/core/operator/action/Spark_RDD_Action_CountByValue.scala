package com.atguigu.bigdata.spark.core.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_RDD_Action_CountByValue {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("action")

    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 1, 1, 2, 3, 4))

    /**
     * countByValue：统计值出现的次数  value -> num
     */
    val intToLong: collection.Map[Int, Long] = rdd.countByValue()

    println(intToLong)

    sc.stop()

  }

}
