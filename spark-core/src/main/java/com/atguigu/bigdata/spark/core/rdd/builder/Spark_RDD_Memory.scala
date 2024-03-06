package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}


object Spark_RDD_Memory {
  def main(args: Array[String]): Unit = {

    val sc = new SparkConf();
    sc.setMaster("local[*]").setAppName("RDD");

    val context = new SparkContext(sc);

    val seq  = Seq(1,2,3,4,5)
    var list = List("hello java", "hello spark", "hello world", "hello golang")



//    val rdd = context.makeRDD(seq)
//    rdd.collect().foreach(println)

    val rdd1 = context.makeRDD(list,2)
    val value = rdd1.flatMap(_.split(" ")).map((_, 1))

    value.collect().foreach(println)

    context.stop()

  }
}
