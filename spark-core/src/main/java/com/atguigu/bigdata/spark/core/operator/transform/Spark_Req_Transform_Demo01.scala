package com.atguigu.bigdata.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark_Req_Transform_Demo01 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local[*]").setAppName("RDD");
    val sc = new SparkContext(sparkConf);
    /**
     * agent.log 数据格式
     *    时间戳，省份，城市，用户，广告，中间字段使用空格分隔。
     * 需求：
     *    统计出每一个省份每个广告被点击数量排行的 Top3
     *
     * 分析 ->
     */
//    1516609143867 6 7 64 16

    val logRDD: RDD[String] = sc.textFile("datas/agent.log")


    //1、过滤无用的数据，并转换成想要的数据结构  ->  ((省份, 地市), 1)
    val rdd1: RDD[((String, String), Int)] = logRDD.map(str => {
      val strArr: Array[String] = str.split(" ")
      ((strArr.apply(1), strArr.apply(4)), 1)
    })

    //2、分组聚合:  ((省份, 地市), 1) -> ((省份, 地市), N)
    val rdd2: RDD[((String, String), Int)] = rdd1.reduceByKey(_ + _)


    //3、((省份, 地市), N)  -> (省份, (地市, N))
    val rdd3: RDD[(String, (String, Int))] = rdd2.map(data => {
      (data._1._1, (data._1._2, data._2))
    })

    //4、按省份分组
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = rdd3.groupByKey()

    //5、组内倒序排序，取前三
    val result: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(e => {
      e.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
    })

    //结果
    result.collect().foreach(println)


    sc.stop()

  }
}
