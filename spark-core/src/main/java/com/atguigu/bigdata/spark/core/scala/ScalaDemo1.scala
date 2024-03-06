package com.atguigu.bigdata.spark.core.scala

object ScalaDemo1 {

  def main(args: Array[String]): Unit = {
    val iteArr: Iterator[Int] = Iterator(1, 2, 3, 4, 5, 6)

//    var i = 0;
//    while(iteArr.hasNext &&  (i < iteArr.size)){
//      println(iteArr.indexOf(i))
//      i += 1;
//      println(i)
//    }
    iteArr.indexOf(2)

    iteArr.foreach(println)



  }



}
