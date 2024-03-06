package com.atguigu.bigdata.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}


object Spark_RDD_Transform_PartitionBy {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local[*]").setAppName("RDD");

    val sc = new SparkContext(sparkConf);

    val rdd = sc.makeRDD(List(1,2,3,4,5,6),2)

    /**
     * partitionBy 数据重分区
     *  必须是键值对
     *
     */
    val pbRDD: RDD[(Int, Null)] = rdd.map((_, null)).partitionBy(new HashPartitioner(2))

    pbRDD.saveAsTextFile("output");
    sc.stop()

  }
}
