package com.atguigu.bigdata.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark_RDD_Transform_Join {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local[*]").setAppName("RDD");

    val sc = new SparkContext(sparkConf);

    val rdd1 = sc.makeRDD(List(("a",1),("a",2),("b",3),("c",4)))
    val rdd2 = sc.makeRDD(List(("a","a1"),("a","a2"),("d",1)))
//    val rdd2 = sc.makeRDD(List(("d",1)))
    /**
     *  join 内连接，笛卡尔乘积
     *  leftOuterJoin 左外连接，左RDD全部保留+笛卡尔乘积
     *  rightOuterJoin 右外连接，左RDD全部保留+笛卡尔乘积
     *
     */
//    rdd1.join(rdd2).collect().foreach(println)
//    rdd1.leftOuterJoin(rdd2).foreach(println)
    rdd1.rightOuterJoin(rdd2).foreach(println)

    sc.stop()

  }
}
