package com.atguigu.bigdata.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark_RDD_Transform_SortBy {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local[*]").setAppName("RDD");

    val sc = new SparkContext(sparkConf);

    val rdd = sc.makeRDD(List(6,2,4,3,1,5),2)
    /**
     * 排序, 默认升序，第二个参数false则为降序
     */
    val sortRDD: RDD[Int] = rdd.sortBy(num => num,false)

    sortRDD.collect().foreach(println)
    println("---------------------------------------------")

    val tupleRdd: RDD[(String, Int)] = sc.makeRDD(List(("A", 1),("A", -3), ("B", 2), ("B", 0), ("C", 3), ("C", -1)))


    val tRdd: RDD[(String, Int)] = tupleRdd.sortBy(e => e._2,false)

    tRdd.collect().foreach(println)
    sc.stop()

  }
}
