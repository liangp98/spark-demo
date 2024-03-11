package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark_ACC {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("acc")

    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    val sumAcc: LongAccumulator = sc.longAccumulator("sum")
//    val sumAcc: LongAccumulator = sc.doubleAccumulator("sum")
//    val sumAcc: LongAccumulator = sc.collectionAccumulator("sum")


    rdd.foreach(num => {
      sumAcc.add(num)
    })

    //获取累加器的值
    println("结果："+sumAcc.value)

  }

}
