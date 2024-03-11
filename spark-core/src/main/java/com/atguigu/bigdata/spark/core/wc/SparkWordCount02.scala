package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object SparkWordCount02 {

  def main(args: Array[String]): Unit = {
    // 创建 Spark 运行配置对象
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    // 创建 Spark 上下文环境对象（连接对象）
    val sc : SparkContext = new SparkContext(sparkConf)

//    wordCount1(sc);
//    println("--------------------------")
//    wordCount2(sc);
//    println("--------------------------")
//    wordCount3(sc);
//    println("--------------------------")
//    wordCount4(sc);
//    println("--------------------------")
//    wordCount5(sc);
//    println("--------------------------")
//    wordCount6(sc);
//    println("--------------------------")
//    wordCount7(sc);
//    println("--------------------------")
//    wordCount8(sc);

    println("--------------------------")
    wordCount9(sc);




    sc.stop()


  }

  def wordCount1(sc : SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Spark", "Hello Scala"))
    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))
    val resultRDD: collection.Map[String, Long] = flatRDD.countByValue()
    println(resultRDD.toList.mkString(","))
  }

  def wordCount2(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Spark", "Hello Scala"))
    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))
    val value: RDD[(String, Int)] = flatRDD.map((_, 1))

    val resultRDD: collection.Map[String, Long] = value.countByKey()
    println(resultRDD.toList.mkString(","))
  }


  def wordCount3(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Spark", "Hello Scala"))
    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))
    val value: RDD[(String, Iterable[String])] = flatRDD.groupBy(_.toString)

    val resultRDD: RDD[(String, Int)] = value.mapValues(
      iter => iter.size
    )
    println(resultRDD.collect().mkString(","))
  }

  def wordCount4(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Spark", "Hello Scala"))
    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))
    val value: RDD[(String, Int)] = flatRDD.map((_, 1))

    val value1: RDD[(String, Iterable[Int])] = value.groupByKey()

    val resultRDD: RDD[(String, Int)] = value1.mapValues(iter => iter.size)

    println(resultRDD.collect().mkString(","))
  }
  def wordCount5(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Spark", "Hello Scala"),2)
    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))
    val value: RDD[(String, Int)] = flatRDD.map((_, 1))

    val resultRDD: RDD[(String, Int)] = value.aggregateByKey(0)(_ + _, _ + _)

    println(resultRDD.collect().mkString(","))
  }
  def wordCount6(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Spark", "Hello Scala"),2)
    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))
    val value: RDD[(String, Int)] = flatRDD.map((_, 1))

    val resultRDD: RDD[(String, Int)] = value.foldByKey(0)(_ + _)

    println(resultRDD.collect().mkString(","))
  }


  def wordCount7(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Spark", "Hello Scala"),2)
    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))
    val value: RDD[(String, Int)] = flatRDD.map((_, 1))

    val resultRDD: RDD[(String, Int)] = value.combineByKey(v=>v,_ + _, _ + _)

    println(resultRDD.collect().mkString(","))
  }

  def wordCount8(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Spark", "Hello Scala"), 2)
    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))
    val value: RDD[(String, Int)] = flatRDD.map((_, 1))

    val resultRDD: RDD[(String, Int)] = value.reduceByKey(_+_)

    println(resultRDD.collect().mkString(","))
  }

  def wordCount9(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Spark", "Hello Scala"), 2)
    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))

    val value: RDD[mutable.Map[String, Long]] = flatRDD.map(
      key => {
        mutable.Map[String, Long]((key, 1))
      //      Map(key -> 1)
    })
    val stringToLong: mutable.Map[String, Long] = value.reduce(
      (map1, map2) => {
        map2.foreach({
          case (word, cout) => {
            val newCount = map1.getOrElse(word, 0L) + cout
//            map1.put(word, newCount)
              map1.update(word,newCount)
          }
        }
        )
        map1
      }
    )

    println(stringToLong.toList.mkString(","))
  }

}
