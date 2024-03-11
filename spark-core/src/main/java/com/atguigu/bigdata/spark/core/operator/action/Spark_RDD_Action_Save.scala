package com.atguigu.bigdata.spark.core.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_RDD_Action_Save {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("action")

    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a" -> 1), ("b" -> 1), ("c" -> 1), ("d" -> 1)), 2)


    rdd.saveAsTextFile("output")
    rdd.saveAsObjectFile("output1")
    //必须是 K V类型才有此方法
    rdd.saveAsSequenceFile("output2")

    sc.stop()

  }

}
