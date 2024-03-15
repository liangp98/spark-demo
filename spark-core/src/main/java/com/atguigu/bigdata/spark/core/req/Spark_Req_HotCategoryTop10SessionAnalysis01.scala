package com.atguigu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext};

object Spark_Req_HotCategoryTop10SessionAnalysis01 {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("save")

    val sc = new SparkContext(sparkConf)

    val srcRDD: RDD[String] = sc.textFile("datas/user_visit_action_src.txt")
    srcRDD.cache()

    //获取top10的cid
    val strings: Array[String] = top10(srcRDD)

    //过滤前10的cid数据
    val filterRDD: RDD[String] = srcRDD.filter(line => {
      val datas: Array[String] = line.split("_")
      if (datas(6) != "-1") {
        strings.contains(datas(6))
      } else
        false
    })

    //组合数据 ((cid,session),1)
    val value: RDD[((String, String), Int)] = filterRDD.map(e => {
      val data: Array[String] = e.split("_")
      (( data(6), data(2)), 1)
    })

    val value1: RDD[((String, String), Int)] = value.reduceByKey(_ + _)

    // 结构转换：((cid,session),1) -> (cid,(session,1)); 分组
    val value2: RDD[(String, Iterable[(String, Int)])] = value1.map({
      case ((cid, session), sum) => {
        (cid, (session, sum))
      }
    }).groupByKey()

    //排序取前10
    val value3: RDD[(String, List[(String, Int)])] = value2.mapValues(iter => {
      iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
    })

    value3.collect().foreach(println)

    sc.stop()

  }

  def top10(srcRDD:RDD[String]): Array[String] = {
    val value: RDD[(String, (Int, Int, Int))] = srcRDD.flatMap(
      line => {
        val str: Array[String] = line.split("_")
        if (str(6) != "-1") {
          //点击
          List((str(6), (1, 0, 0)))
        } else if (str(8) != "null") {
          //下单
          val orderStr: Array[String] = str(8).split(",")
          orderStr.map(m => (m, (0, 1, 0)))
        } else if (str(10) != "null") {
          //支付
          val payStr: Array[String] = str(10).split(",")
          payStr.map(m => (m, (0, 0, 1)))
        } else {
          Nil
        }
      }
    )
    val value1: RDD[(String, (Int, Int, Int))] = value.reduceByKey(
      (map1, map2) => {
        (map1._1 + map2._1, map1._2 + map2._2, map1._3 + map2._3)
      }
    )
    //输出结果
//    value1.sortBy(_._2, false).take(10).foreach(println)

    value1.sortBy(_._2, false).take(10).map(_._1)
  }

}
