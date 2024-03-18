package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming_WordCount {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming")

    //创建Streaming ,每3秒执行一次
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //监听对应ip端口数据
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val value: DStream[String] = lines.flatMap(_.split(" "))

    val wordToOne: DStream[(String, Int)] = value.map((_, 1))

    val result: DStream[(String, Int)] = wordToOne.reduceByKey(_ + _)

    result.print()

    //启动采集器
    ssc.start();
    //等待采集器关闭
    ssc.awaitTermination()
  }

}
