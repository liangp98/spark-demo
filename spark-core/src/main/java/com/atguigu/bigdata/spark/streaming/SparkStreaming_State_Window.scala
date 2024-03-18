package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming_State_Window {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming")

    //创建Streaming ,每3秒执行一次
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("cp")
    //监听对应ip端口数据
    val linesDStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)


    //滑动窗口
    val value: DStream[String] = linesDStream.window(Seconds(12), Seconds(6))
    val wordToCount: DStream[(String, Int)] = value.flatMap(_.split(" ")).map((_, 1))

    wordToCount.reduceByKey(_+_).print()

    //启动采集器
    ssc.start();
    //等待采集器关闭
    ssc.awaitTermination()
  }

}
