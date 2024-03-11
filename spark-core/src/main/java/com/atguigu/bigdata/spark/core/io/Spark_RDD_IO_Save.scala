package com.atguigu.bigdata.spark.core.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_RDD_IO_Save {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("save")

    val sc = new SparkContext(sparkConf)

    //文件读取
    val value: RDD[String] = sc.textFile("output1");
    println(value.collect().mkString(","))

    val value1: RDD[(String, Int)] = sc.objectFile[(String, Int)]("output2")
    println(value1.collect().mkString(","))


    val value2: RDD[(String, Int)] = sc.sequenceFile[String, Int]("output3")
    println(value2.collect().mkString(","))



    //文件写出
//    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
//      ("a", 1),
//      ("b", 2),
//      ("c", 3)
//    ))
//
//    rdd.saveAsTextFile("output1")
//    rdd.saveAsObjectFile("output2")
//    rdd.saveAsSequenceFile("output3")

  }

}
