package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark_ACC_Cus_Accumulate {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("acc")

    val sc = new SparkContext(sparkConf)
    val rdd: RDD[String] = sc.makeRDD(List("Hello","Spark","Hello"))

    val wcAcc = new MyAccumulate()

    sc.register(wcAcc)


//    val wcAcc: LongAccumulator = sc.longAccumulator("wcAcc")

    rdd.foreach(m =>{
      wcAcc.add(m)
    })


    //获取累加器的值
    println("结果："+wcAcc.value)

  }

  /**
   * 自定义累加器
   */
  class MyAccumulate extends AccumulatorV2[String,mutable.Map[String,Long]]{

    private val map = mutable.Map[String,Long]()
    override def isZero: Boolean = {
      map.isEmpty
    }

    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      new MyAccumulate()
    }

    override def reset(): Unit = {
      map.clear()
    }

    override def add(v: String): Unit = {
      val newCount = map.getOrElse(v,0L) + 1
      map.update(v,newCount)
    }

    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      val map1 = this.map
      val map2 = other.value

      map2.foreach(
        m =>{
          val newCount = map1.getOrElse(m._1,0L) + m._2
          map1.update(m._1,newCount)
        }
      )

//      map2.foreach{
//          case (word,count) =>{
//            val newCount= map1.getOrElse(word,0L)+count
//            map1.update(word,newCount)
//          }
//      }



    }

    override def value: mutable.Map[String, Long] = {
      this.map
    }
  }

}
