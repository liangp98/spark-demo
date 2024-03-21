package com.atguigu.bigdata.spark.demo

import cn.hutool.json.{JSONArray, JSONUtil}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import java.text.SimpleDateFormat
import java.util.Date


object Spark_ReplaceProcess_Demo {

  def main(args: Array[String]): Unit = {
    val start: Long = System.currentTimeMillis()

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("staff_login_log")
    val spark: SparkSession = SparkSession.builder()
      .config(sparkConf)
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .getOrCreate()
    import spark.implicits._

    var dayId = 20230318;
    var provinceCode = "8350000"
    var startTime = "2023-03-18 00:00:00"
    var endTime = "2023-03-19 00:00:00"

    val actionSql =
      """
      select
        day_id,
        now() as etl_date,
        '-' as area_code,
        staff_id,
        start_time,
        end_time,
        ip_addr,
        round(COALESCE(EXTRACT(EPOCH FROM (end_time - start_time)),0)::numeric/60,2) as login_time,
        '-' as staff_area_code,
        action_id,
        action_type_id,
        CASE
        when action_id = 0 then '0'
        when action_id = 1 and action_type_id = 0 then '100'
        when action_id = 1 and action_type_id = 1 then '101'
        when action_id = 1 and action_type_id = 2 then '102'
        when action_id = 1 and action_type_id = 3 then '103'
        when action_id = 1 and action_type_id = 4 then '104'
        when action_id = 1 and action_type_id = 5 then '105'
        when action_id = 1 and action_type_id = 6 then '106'
        when action_id = 1 and action_type_id = 7 then '107'
        when action_id = 1 and action_type_id = 8 then '108'
        when action_id = 1 and action_type_id = 10 then '110'
        when action_id = 1 and action_type_id = 11 then '111'
        when action_id = 1 and action_type_id = 15 then '115'
        when action_id = 2 then '200'
        when action_id = 3 then '300'
        when action_id = 4 then '400'
        else '-' END as action_name
      from dwd_staff_action_log_8350000_202303_bak
      where day_id=""" + dayId +
        """
      and province_code='""" + provinceCode +
        """'
      and start_time>='""" + startTime +
        """'
      and start_time<'""" + endTime +
        """'
    """

    //临时表1
    createActionTempTable("action_temp_01",spark,actionSql)
    val action: DataFrame = spark.sql("select * from action_temp_01")

    //临时表2
    val staffSql =
      """
      select a.*  from (select *,row_number() OVER (PARTITION BY staff_id order by staff_org_status desc,team_code desc) row_id from tf_staff_org_region_rel ) a where row_id=1
      """
    createSeatTempTable("staff_temp_01",spark,staffSql)
    val staff: DataFrame = spark.sql("select * from staff_temp_01")

    //临时表3
    val regionSql = "select * from ts_org"
    createRegionTempTable("region_temp_01",spark,regionSql)
    val region: DataFrame = spark.sql("select * from region_temp_01")


    val sql =
      """
      select
                a.day_id,
                to_timestamp(a.etl_date,'yyyy-MM-dd HH:mm:ss') as etl_date,
                case when b.region_code is null then '-' else b.region_code end as region_code ,
                b.province_code,
                b.city_code,
                b.staff_name,
                b.team_name as org_name,
                b.team_code as org_code,
                b.hr_code,
                a.area_code,
                a.staff_id,
                to_timestamp(a.start_time,'yyyy-MM-dd HH:mm:ss') as start_time,
                to_timestamp(a.end_time,'yyyy-MM-dd HH:mm:ss') as end_time,
                a.ip_addr,
                a.login_time,
                case when b.region_code is null then '-' else b.region_code end as staff_region_code,
                b.region_name as staff_region_name,
                a.staff_area_code,
                f.full_name as org_full_path,
                f.org_full_id_path as org_full_id_path,
                b.province_name,
                b.city_name,
                b.center_code,
                b.center_name,
                a.action_id,
                a.action_type_id,
                a.action_name,
                b.org_name as org_name1,
                b.parent_org_name
          from action_temp_01 a
          left join staff_temp_01 b on a.staff_id = b.staff_id
          left join region_temp_01 f on f.org_code = b.team_code AND f.status = 'E'
      """
    val frame: DataFrame = spark.sql(sql)

    println("------开始写入-------")
    writeToDB(frame,"ads_staff_login_log_202404")
    println("------写入完成-------")

    val end: Long = System.currentTimeMillis()
    println("------耗时-------" + (end-start)/1000 +"s")



  }

  def createActionTempTable(tableName: String, spark: SparkSession, sql: String): Unit = {
    import spark.implicits._
    //员工动作表
    val srcData: DataFrame = getDataFrameByTableName(spark, sql)
    val entities: Array[actionEntity] = srcData.rdd.map(e => {
      val t: Array[String] = e.toString().substring(1, e.toString().length - 1).split(",")
      t.update(7, t.toList.apply(7).toDouble.toString)
      actionEntity(t.apply(0).toInt, t.apply(1), t.apply(2), t.apply(3), t.apply(4), t.apply(5), t.apply(6), t.apply(7).toDouble, t.apply(8),
        t.apply(9).toInt, t.apply(10).toInt, t.apply(11))
    }).collect()

    val value: RDD[actionEntity] = spark.sparkContext.makeRDD(entities)
    //临时表1
    val frame: DataFrame = value.toDF()
    frame.createOrReplaceTempView(tableName);

  }

  def createSeatTempTable(tableName:String,spark: SparkSession, sql: String): Unit = {

    val frame2: DataFrame = getDataFrameByTableName(spark, sql)
    frame2.createOrReplaceTempView(tableName)
  }

  def createRegionTempTable(tableName: String, spark: SparkSession, sql: String): Unit = {

    val frame2: DataFrame = getDataFrameByTableName(spark, sql)
    frame2.createOrReplaceTempView(tableName)
  }




  def getDataFrameByTableName(spark: SparkSession, sql: String): DataFrame = {
    val frame = spark.read.format("jdbc")
      .option("url", "jdbc:postgresql://10.11.1.101:16601/fj10000jy?currentSchema=jy_report&useUnicode=true&characterEncoding=utf8&useSSL=false")
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", "(" + sql + ") t")
      .option("user", "xy10000jy_test")
      .option("password", "FF10000_20220623#pg")
      .load()
    frame
  }

  def writeToDB(df: DataFrame, tableName: String): Unit = {
    val frame = df.write.format("jdbc")
      .mode(SaveMode.Append)
      .option("url", "jdbc:postgresql://10.11.1.101:16601/fj10000jy?currentSchema=jy_report&useUnicode=true&characterEncoding=utf8&useSSL=false")
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", tableName)
      .option("user", "xy10000jy_test")
      .option("password", "FF10000_20220623#pg")
      .save()
  }


  case class actionEntity(
                           day_id: Int,
                           etl_date: String,
                           area_code: String,
                           staff_id: String,
                           start_time: String,
                           end_time: String,
                           ip_addr: String,
                           login_time: Double,
                           staff_area_code: String,
                           action_id: Int,
                           action_type_id: Int,
                           action_name: String
                         )

}
