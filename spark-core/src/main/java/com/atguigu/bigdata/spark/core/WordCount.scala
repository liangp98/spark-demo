package com.atguigu.bigdata.spark.core

object WordCount {
  def main(args: Array[String]): Unit = {

    val srcList = List("hello java","hello scala","hello world","hello spark from scala","hello flink from scala");


    val flatList = srcList.flatMap(_.split(" "))

    val groupMap = flatList.groupBy(item => item)
    println(groupMap)
    val stringToStrings: Map[String, Int] = groupMap.map(kv => (kv._1, kv._2.size))
    println(stringToStrings)

    val sortList = stringToStrings.toList.sortWith(_._2 > _._2).take(3)
    println(sortList)

  }

}
