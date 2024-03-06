package com.atguigu.bigdata.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}


object Spark_RDD_Transform_AggregateByKey01 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local[*]").setAppName("RDD");

    val sc = new SparkContext(sparkConf);

    val rdd = sc.makeRDD(List(("a",1),("a",2),("b",3),("b",4),("b",5),("a",6)),2)

    /**
     *  需求 分区内求最大值，分区间求和。结果：("a",8),("b",8)
     *
     *
     * aggregateByKey 自定义分组聚合规则
     *  参数一： 与之运算的初始值
     *  参数二：
     *    ---参数1：各分区内的计算规则
     *    ---参数2：各分区间的计算规则
     */
    rdd.aggregateByKey(0)(
      (x:Int,y:Int)=>Math.max(x,y),
      (x:Int,y:Int)=>x+y
    ).collect().foreach(println)


    sc.stop()

  }
}
