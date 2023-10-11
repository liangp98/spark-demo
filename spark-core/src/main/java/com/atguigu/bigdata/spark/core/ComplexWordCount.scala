package com.atguigu.bigdata.spark.core

object ComplexWordCount {
  def main(args: Array[String]): Unit = {

    val srcList = List(
      ("hello java",1),
      ("hello scala",2),
      ("hello world",3),
      ("hello spark from scala",1),
      ("hello flink from scala",2)
    );


    val flatList: List[(String,Int)] = srcList.flatMap(o =>o._1.split(" ").map(k =>(k,o._2)))
    println(flatList)
    val groupList = flatList.groupBy(_._1)
    println(groupList)

    val wordMap: Map[String, Int] = groupList.mapValues(groupList => groupList.map(_._2).sum)
    println(wordMap)

    val resultList: List[(String, Int)] = wordMap.toList.sortWith(_._2 > _._2).take(3)
    println(resultList)




//    val groupMap = flatList.groupBy(item => item)
//    println(groupMap)
//    val stringToStrings: Map[String, Int] = groupMap.map(kv => (kv._1, kv._2.size))
//    println(stringToStrings)
//
//    val sortList = stringToStrings.toList.sortWith(_._2 > _._2).take(3)
//    println(sortList)

  }

}
