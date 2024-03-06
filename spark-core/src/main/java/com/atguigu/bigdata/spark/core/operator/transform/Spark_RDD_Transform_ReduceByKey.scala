package com.atguigu.bigdata.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark_RDD_Transform_ReduceByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local[*]").setAppName("RDD");

    val sc = new SparkContext(sparkConf);

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("b", 4)))
    /**
     * reduceByKey: 将相同key的value进行分组聚合操作
     *
     */
    val reduceByKeyRDD: RDD[(String, Int)] = rdd.reduceByKey((x: Int, y: Int) => {
      println("x=" + x + ",y=" + y)
      x + y
    })
//    val reduceByKeyRDD = rdd.reduceByKey((x,y)=>x+y);
//    val reduceByKeyRDD = rdd.reduceByKey(_+_);
    reduceByKeyRDD.collect().foreach(println)

    sc.stop()

  }
}
