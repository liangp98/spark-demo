package com.atguigu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable;

object Spark_Req_HotCategoryTop10Analysis02 {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("save")

    val sc = new SparkContext(sparkConf)

    val hc = new HotCategoryAccumulate()
    sc.register(hc, "hc")


    //获取数据
    val srcRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")
    //处理点击，下单，支付数据
    srcRDD.foreach(
      line => {
        val str: Array[String] = line.split("_")
        if (str(6) != "-1") {
          //点击
          hc.add((str(6), "click"))
        } else if (str(8) != "null") {
          //下单
          val orderStr: Array[String] = str(8).split(",")
          orderStr.foreach(m =>
            hc.add((m, "order"))
          )
        } else if (str(10) != "null") {
          //支付
          val payStr: Array[String] = str(10).split(",")
          payStr.foreach(m =>
            hc.add((m, "pay"))
          )
        }
      }
    )

    //输出结果
    //    value1.sortBy(_._2, false).take(10).foreach(println)
    val values: Iterable[HotCategory] = hc.value.values
//    val values: mutable.Iterable[HotCategory] = hc.value.map(_._2)
    values.toList.sortWith((left, right) => {
      if(left.clickCnt>right.clickCnt){
        true
      }else if(left.clickCnt == right.clickCnt){
        if(left.orderCnt > right.orderCnt){
          true
        }else if(left.payCnt > right.payCnt){
          true
        }else
          false
      }else
        false
    }).take(10).foreach(println)

    sc.stop()
  }

  case class HotCategory(cid: String,var clickCnt: Int, var orderCnt: Int, var payCnt: Int)

  class HotCategoryAccumulate extends AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] {
    private val hcMap: mutable.Map[String, HotCategory] = mutable.Map[String, HotCategory]()

    override def isZero: Boolean = hcMap.isEmpty

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = new HotCategoryAccumulate()

    override def reset(): Unit = hcMap.clear()

    override def add(v: (String, String)): Unit = {
      var cid = v._1;
      var action = v._2;
      val category = hcMap.getOrElse(cid, HotCategory(cid,0, 0, 0))
      action match {
        case "click" => category.clickCnt += 1
        case "order" => category.orderCnt += 1
        case "pay" => category.payCnt += 1
      }
      hcMap.update(cid, category)
    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
      var m1 = this.hcMap
      var m2 = other.value

      //      m2.foreach({
      //        case (cid, hc) => {
      //          val category: HotCategory = m1.getOrElse(cid, HotCategory(0, 0, 0))
      //          category.clickCnt += hc.clickCnt
      //          category.orderCnt += hc.orderCnt
      //          category.payCnt += hc.payCnt
      //          m1.update(cid, category)
      //        }
      //      })

      m2.foreach {
        case (cid, hc) => {
          val category: HotCategory = m1.getOrElse(cid, HotCategory(cid,0, 0, 0))
          category.clickCnt += hc.clickCnt
          category.orderCnt += hc.orderCnt
          category.payCnt += hc.payCnt
          m1.update(cid, category)
        }
      }

    }


    override def value: mutable.Map[String, HotCategory] = this.hcMap
  }

}
