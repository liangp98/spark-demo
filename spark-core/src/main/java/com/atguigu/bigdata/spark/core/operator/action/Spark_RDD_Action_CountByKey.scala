package com.atguigu.bigdata.spark.core.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_RDD_Action_CountByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("action")

    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 1), ("a", 1), ("b", 1), ("b", 1)))

    /**
     *  统计Map 中 key的数量
     *
     */
    val map: collection.Map[String, Long] = rdd.countByKey()
    println(map)

    sc.stop()

  }

}
