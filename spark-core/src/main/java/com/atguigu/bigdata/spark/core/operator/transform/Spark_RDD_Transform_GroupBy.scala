package com.atguigu.bigdata.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark_RDD_Transform_GroupBy {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local[*]").setAppName("RDD");

    val sc = new SparkContext(sparkConf);

    val data = List(1,2,3,4,5,6)

    val rdd = sc.makeRDD(data)

    /**
     * 每个元素所在的分区索引好
     */
    val groupRdd: RDD[(String, Iterable[Int])] = rdd.groupBy(item => {
      if (item % 2 == 0) {
        "偶数"
      } else {
        "奇数"
      }
    })
    groupRdd.collect().foreach(println)

    println("-------------------------------")

    val grdd: RDD[(Char, Iterable[String])] = sc.makeRDD(List("Hello", "Spark", "Scala", "Hadoop")).groupBy(_.charAt(0))
    grdd.collect().foreach(println)

    val value: RDD[(Char, List[String])] = grdd.map(e => (e._1, e._2.iterator.toList))
    value.collect().foreach(println)

    sc.stop()

  }
}
