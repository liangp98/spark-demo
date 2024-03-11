package com.atguigu.bigdata.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark_RDD_Transform_AggregateByKey02 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local[*]").setAppName("RDD");

    val sc = new SparkContext(sparkConf);

    val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("b", 4), ("b", 5), ("a", 6)), 2)

    /**
     * 需求 求相同key的平均值
     *
     *
     * aggregateByKey 自定义分组聚合规则
     * 参数一： 与之运算的初始值
     * 参数二：
     * ---参数1：各分区内的计算规则
     * ---参数2：各分区间的计算规则
     */
    val value: RDD[(String, (Int, Int))] = rdd.aggregateByKey((0, 0))(
      (U, V) => (U._1 + V, U._2 + 1),
      (U1, U2) => (U1._1 + U2._1, U1._2 + U2._2)
    )
    println(value.collect().mkString(","))

    //mapValues实现
    var result = value.mapValues({
      case (a, b) => {
        a / b
      }
    })
    //map实现
//    val value1: RDD[(String, Int)] = value.map(k => {
//      (k._1, k._2._1 / k._2._2)
//    })
//    println(value1.collect().mkString(","))

//
//    var result = value.mapValues{
//      case (a,b) =>{
//        a/b
//      }
//    }
    result.collect().foreach(println)

    sc.stop()

  }
}
