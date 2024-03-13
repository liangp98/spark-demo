package com.atguigu.bigdata.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSQL_Basic {
  def main(args: Array[String]): Unit = {
    //创建SparkSQL环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._ //session变量的名字  import xx.implicits._

    //执行逻辑
    /**
     * DataFrame
     */
    val df: DataFrame = spark.read.json("datas/user.json")
    //    df.show()

    /**
     * DataFrame => SQL
     */
    df.createOrReplaceTempView("user") //创建临时表
    spark.sql("select * from user").show()
    spark.sql("select age,username from user").show()
    spark.sql("select avg(age) from user").show()

    /**
     * DataFrame => DSL
     */
    df.select("username", "age").show()
    df.select($"age" + 1).show()
    df.select('age + 2).show()

    /**
     * DataSet
     */
    val list: List[Int] = List(1, 2, 3, 4)
    list.toDS().show()

    /**
     * RDD <=> DataFrame
     */
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 20), (2, "lisi", 23)))
    val dfRdd: DataFrame = rdd.toDF("id", "name", "age")
    dfRdd.show()
    //    val rdd1: RDD[Row] = dfRdd.rdd
    //    rdd1.foreach(println)

    /**
     * DataFrame <=> DataSet
     */
    val ds: Dataset[User] = dfRdd.as[User]
    ds.show()
//    val frame: DataFrame = ds.toDF()
    //    frame.show()

    /**
     * RDD <=> DataSet
     */
    val ds1: Dataset[User] = rdd.map {
      case (id, name, age) => User(id, name, age)
    }.toDS()

//    val rdd1: RDD[User] = ds1.rdd


    //关闭连接
    spark.close()


  }

  case class User(id: Int, name: String, age: Int)

}
