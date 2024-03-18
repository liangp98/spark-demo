package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming_State {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming")

    //创建Streaming ,每3秒执行一次
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("cp")
    //监听对应ip端口数据
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)


    val wordToOne: DStream[(String, Int)] = lines.map((_, 1))


    val value1: DStream[(String, Int)] = wordToOne.updateStateByKey(
      (seq: Seq[Int], opt: Option[Int]) => {
        val value: Int = opt.getOrElse(0) + seq.sum
        Option(value)
      }
    )

    value1.print()

    //启动采集器
    ssc.start();
    //等待采集器关闭
    ssc.awaitTermination()
  }

}
