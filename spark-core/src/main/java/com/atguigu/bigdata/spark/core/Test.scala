package com.atguigu.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Test {
    def main(args: Array[String]): Unit = {
      val sparkConf = new SparkConf()
      sparkConf.setAppName("case_test")
      sparkConf.setMaster("local[2]")
      val sparkContext = new SparkContext(sparkConf)
      val rdd = sparkContext.textFile("datas/people.txt")
      rdd.map(line=>line.split(","))
        .map(
          line=>if(line.length == 1) (line(0))
          else if(line.length == 2) (line(0),line(1))
          else (line(0),line(1),line(2))
        )
        .map{
          case (one) => ("one:"+one)
          case (name,age) =>("name:"+name,"age:"+age)
          case _ => ("_name","_age","_")
        }
        .foreach(println)
    }


}
