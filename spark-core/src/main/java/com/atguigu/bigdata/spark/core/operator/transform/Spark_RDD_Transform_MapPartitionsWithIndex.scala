package com.atguigu.bigdata.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}


object Spark_RDD_Transform_MapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local[*]").setAppName("RDD");

    val sc = new SparkContext(sparkConf);

    val rdd = sc.makeRDD(List(1, 2, 3, 4),2)

    /**
     * 返回第二个分区的数据
     */
    val rdd2 = rdd.mapPartitionsWithIndex(
      (index,iter) => {
        if(index==1){
          List(iter.max).iterator
        }else{
          Nil.iterator;
        }
      })

    rdd2.collect().foreach(println)
    sc.stop()

  }
}
