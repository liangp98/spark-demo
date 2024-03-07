package com.atguigu.bigdata.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}


object Spark_RDD_Transform_Cogroup {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local[*]").setAppName("RDD");

    val sc = new SparkContext(sparkConf);

    val rdd1 = sc.makeRDD(List(("a",1),("a",2),("b",3),("c",4)))
    val rdd2 = sc.makeRDD(List(("a",200),("d",1),("d",32)))
    /**
     *  cogroup: connect + group  ->连接 + 分组
     */
//    rdd1.join(rdd2).collect().foreach(println)
//    rdd1.leftOuterJoin(rdd2).foreach(println)
    rdd1.cogroup(rdd2).foreach(println)

    sc.stop()

  }
}
