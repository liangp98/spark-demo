package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming_State_Join {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming")

    //创建Streaming ,每3秒执行一次
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("cp")
    //监听对应ip端口数据
    val lineDStream1: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val lineDStream2: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 8888)
    //转换为 RDD 操作
    val wordToOne1: DStream[(String, Int)] = lineDStream1.flatMap(_.split(" ")).map((_, 1))
    val wordToOne2: DStream[(String, Int)] = lineDStream2.flatMap(_.split(" ")).map((_, 1))

    val wordAndCountDStream: DStream[(String, (Int, Int))] = wordToOne1.join(wordToOne2)
    //打印
    wordAndCountDStream.print()
    //启动采集器
    ssc.start();
    //等待采集器关闭
    ssc.awaitTermination()
  }

}
