package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

object SparkWordCount_Cus_Partitioner {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("cusPartitioner")
    val sc : SparkContext = new SparkContext(sparkConf)
    val value: RDD[(String, String)] = sc.makeRDD(List(
      ("nba", "jfa"),
      ("wnba", "ttw"),
      ("cba", "hhh"),
      ("nba", "qec")
    ))
    val value1: RDD[(String, String)] = value.partitionBy(new MyPartitioner(3))

    value1.saveAsTextFile("output")

  }

  /**
   * 自定义分区器
   */
  private class MyPartitioner(partitions: Int) extends Partitioner{
    override def numPartitions: Int = partitions

    override def getPartition(key: Any): Int = {
      key match {
        case "nba" => 0
        case "wnba" => 1
        case _ => 2
      }
    }
  }

}
