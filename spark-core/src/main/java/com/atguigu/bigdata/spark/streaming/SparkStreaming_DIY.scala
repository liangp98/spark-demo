package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.util.Random

object SparkStreaming_DIY {

  def main(args: Array[String]): Unit = {

    //1.初始化 Spark 配置信息
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDDStream")
    //2.初始化 SparkStreamingContext
    val ssc = new StreamingContext(conf, Seconds(4))

    val value: ReceiverInputDStream[String] = ssc.receiverStream(new CusMyReceiver())
    value.print()

    //7.启动任务
    ssc.start()
    ssc.awaitTermination()
  }
  class CusMyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY){
    var continueFlag = true;
    override def onStart(): Unit = {
      new Thread(new Runnable {
        override def run(): Unit = {
          while (continueFlag) {
            val message: String = "采集的数据为：" + new Random().nextInt(100).toString
            Thread.sleep(500L)
            store(message)
          }
        }
      }).start()
    }

    override def onStop(): Unit = {
      continueFlag = false
    }
  }
}
