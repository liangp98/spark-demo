package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming_State_Transform {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming")

    //创建Streaming ,每3秒执行一次
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("cp")
    //监听对应ip端口数据
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    //转换为 RDD 操作
    val wordAndCountDStream: DStream[(String, Int)] = lineDStream.transform(rdd =>
    {
      val words: RDD[String] = rdd.flatMap(_.split(" "))
      val wordAndOne: RDD[(String, Int)] = words.map((_, 1))
      val value: RDD[(String, Int)] = wordAndOne.reduceByKey(_ + _)
      value
    })
    //打印
    wordAndCountDStream.print
    //启动采集器
    ssc.start();
    //等待采集器关闭
    ssc.awaitTermination()
  }

}
