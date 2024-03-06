package com.atguigu.bigdata.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}


object Spark_RDD_Transform_FlatMap {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local[*]").setAppName("RDD");

    val sc = new SparkContext(sparkConf);

    val data = List(List(1,2,3),List(4,5,6,7))

    val rdd = sc.makeRDD(data)

    /**
     * 每个元素所在的分区索引好
     */
    val rdd2 = rdd.flatMap(l =>{
      l.map(_*2)
    });

    rdd2.collect().foreach(println)



    sc.stop()

  }
}
