package com.atguigu.bigdata.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark_RDD_Transform_AggregateByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local[*]").setAppName("RDD");

    val sc = new SparkContext(sparkConf);

    val rdd = sc.makeRDD(List(("a",1),("a",2),("a",3),("a",4)),2)

    /**
     *  需求 分区内求最大值，分区间求和。结果：("a",6)
     *  步骤：("a",1),("a",2),("a",3),("a",4) ->   ("a",2),("a",4) -> ,("a",6)
     *
     * aggregateByKey 自定义分组聚合规则
     *  参数一： 与之运算的初始值
     *  参数二：
     *    ---参数1：各分区内的计算规则
     *    ---参数2：各分区间的计算规则
     */
    rdd.aggregateByKey(0)(
      (u:Int,v:Int)=>Math.max(u,v),
      (u1:Int,u2:Int)=>u1+u2
    ).collect().foreach(println)


    sc.stop()

  }
}
