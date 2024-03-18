package com.atguigu.bigdata.spark.streaming

import com.atguigu.bigdata.spark.streaming.SparkStreaming_DIY.CusMyReceiver
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets


/***
 * 自定义数据源，实现监控某个端口号，获取该端口号内容。
 */
object SparkStreaming_DIY01 {


  def main(args: Array[String]): Unit = {

    //1.初始化 Spark 配置信息
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDDStream")
    //2.初始化 SparkStreamingContext
    val ssc = new StreamingContext(conf, Seconds(3))

    val value: ReceiverInputDStream[String] = ssc.receiverStream(new CustomerReceiver("localhost",9999))
    value.print()

    //7.启动任务
    ssc.start()
    ssc.awaitTermination()
  }

  class CustomerReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
    override def onStop(): Unit = {
    }
    //最初启动的时候，调用该方法，作用为：读数据并将数据发送给 Spark

    override def onStart(): Unit = {
      new Thread("Socket Receiver") {
        override def run() {
          receive()
        }
      }.start()
    }

    //读数据并将数据发送给 Spark
    def receive(): Unit = {
      //创建一个 Socket
      var socket: Socket = new Socket(host, port)
      //定义一个变量，用来接收端口传过来的数据
      var input: String = null
      //创建一个 BufferedReader 用于读取端口传来的数据
      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8))
      //读取数据
      input = reader.readLine()
      //当 receiver 没有关闭并且输入数据不为空，则循环发送数据给 Spark
      while (!isStopped() && input != null) {
        store(input)
        input = reader.readLine()
      }
      //跳出循环则关闭资源
      reader.close()
      socket.close()
      //重启任务
      restart("restart")
    }

  }
}
