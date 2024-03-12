package com.atguigu.bigdata.spark.core.broadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark_BC {


  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("broadcast")

    val sc = new SparkContext(sparkConf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))

    val map: mutable.Map[String, Int] = mutable.Map[String, Int](("a", 4), ("b", 5), ("c", 6))

    /**
     * 声明广播变量：分布式只读变量,存在Executor中
     */
    val bcMap: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map)


    rdd1.map {
      case (key, value) => {
//        val v: Int = map.getOrElse(key, 0)  //普通
        val v: Int = bcMap.value.getOrElse(key, 0)    //广播变量
        (key, (value, v))
      }
    }.foreach(println)


    sc.stop()


  }

}
