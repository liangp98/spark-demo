package com.atguigu.bigdata.spark.core.scala

import scala.collection.mutable

object ScalaMap {
  def main(args: Array[String]): Unit = {
    val map: mutable.Map[String, Int] = mutable.Map("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4, "e" -> 5)

    println(map)
//    var m = map + ("g"->5)
    map.put("a",100)
    println(map)
    map.update("a",1001)
    println(map)

  }

}