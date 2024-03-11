package com.atguigu.bigdata.spark.core.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_RDD_Persist_Cache {
  def main(args: Array[String]): Unit = {
    /**
     * 问题发现：观察 '@@@@@' 打印的次数
     *  reduceByKey 和 groupByKey虽然复用tupleRDD 但是本质上都是要重新获取基础数据加工计算的;
     *  仅仅只是表面复用
     */

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")

    val sc = new SparkContext(sparkConf)


    val rdd: RDD[String] = sc.makeRDD(List("Hello Spark", "Hello Scala", "Hello Hadoop"))

    val tupleRDD: RDD[(String, Int)] = rdd.flatMap(_.split(" ")).map(
      m=>{
        println("@@@@@")  //打印
        (m, 1)
      }
    )

    val reduceRDD: RDD[(String, Int)] = tupleRDD.reduceByKey(_ + _)

    reduceRDD.collect().foreach(println)

    println("*******************************")


    val groupRDD: RDD[(String, Iterable[Int])] = tupleRDD.groupByKey()

    groupRDD.collect().foreach(println)

  }
}
