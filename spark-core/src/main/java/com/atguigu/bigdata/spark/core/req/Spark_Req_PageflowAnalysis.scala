package com.atguigu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext};

object Spark_Req_PageflowAnalysis {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("save")

    val sc = new SparkContext(sparkConf)

    val srcRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")
    val dataPageRDD: RDD[UserVisitAction] = srcRDD.map(line => {
      val data: Array[String] = line.split("_")
      UserVisitAction(
        data(0),
        data(1).toLong,
        data(2),
        data(3).toLong,
        data(4),
        data(5),
        data(6).toLong,
        data(7).toLong,
        data(8),
        data(9),
        data(10),
        data(11),
        data(12).toLong
      )
    })
    dataPageRDD.cache()

    //分母
    val parent: RDD[(Long, Long)] = dataPageRDD.map(o => (o.page_id, 1L)).reduceByKey(_ + _)
    val pageidToCountMap: Map[Long, Long] = parent.collect().toMap
    //分子

    val value: RDD[(String, Iterable[UserVisitAction])] = dataPageRDD.groupBy(_.session_id)
    val mvRDD: RDD[(String, List[((Long, Long), Int)])] = value.mapValues(iter => {
      //按时间排序
      val sortList: List[UserVisitAction] = iter.toList.sortBy(_.action_time)

      val pageIds: List[Long] = sortList.map(_.page_id)
      val tuples: List[(Long, Long)] = pageIds.zip(pageIds.tail)
      val tuples1: List[((Long, Long), Int)] = tuples.map(t => (t, 1))
      tuples1
    })


    val flatRDD: RDD[((Long, Long), Int)] = mvRDD.map(_._2).flatMap(list => list)
    val dataRDD = flatRDD.reduceByKey(_ + _)

    // TODO 计算单跳转换率
    // 分子除以分母
    dataRDD.foreach {
      case ((pageid1, pageid2), sum) => {
        val lon: Long = pageidToCountMap.getOrElse(pageid1, 0L)

        println(s"页面${pageid1}跳转到页面${pageid2}单跳转换率为:" + (sum.toDouble / lon))
      }
    }

    sc.stop()

  }

  //用户访问动作表
  case class UserVisitAction(
    date: String, //用户点击行为的日期
    user_id: Long, //用户的 ID
    session_id: String, //Session 的 ID
    page_id: Long, //某个页面的 ID
    action_time: String, //动作的时间点
    search_keyword: String, //用户搜索的关键词
    click_category_id: Long, //某一个商品品类的 ID
    click_product_id: Long, //某一个商品的 ID
    order_category_ids: String, //一次订单中所有品类的 ID 集合
    order_product_ids: String, //一次订单中所有商品的 ID 集合
    pay_category_ids: String, //一次支付中所有品类的 ID 集合
    pay_product_ids: String, //一次支付中所有商品的 ID 集合
    city_id: Long
  ) //城市 id
}
