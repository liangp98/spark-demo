package com.atguigu.bigdata.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark_RDD_Transform_JCB {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local[*]").setAppName("RDD");

    val sc = new SparkContext(sparkConf);

    val rdd1 = sc.makeRDD(List(1,2,3,4))
    val rdd2 = sc.makeRDD(List(3,4,5,6))
//    val rdd2 = sc.makeRDD(List("a","b","c","d"))


    //交集
    val intersectionRDD: RDD[Int] = rdd1.intersection(rdd2)
//    intersectionRDD.collect().foreach(println)
    println(intersectionRDD.collect().mkString(","))
    println("------------------------------")
    //差集
    val subtractRDD: RDD[Int] = rdd1.subtract(rdd2)
//    subtractRDD.collect().foreach(println)
    println(subtractRDD.collect().mkString(","))
    println("------------------------------")

    //并集
    val unionRDD: RDD[Int] = rdd1.union(rdd2)
//    unionRDD.collect().foreach(println)
    println(unionRDD.collect().mkString(","))
    println("------------------------------")

    //拉链
    val zipRDD: RDD[(Int, Int)] = rdd1.zip(rdd2)
    println(zipRDD.collect().mkString(","))
    sc.stop()

  }
}
