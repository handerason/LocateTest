package com.bonc.ServerTest



import java.text.SimpleDateFormat
import java.util.Date

import com.bonc.data.{LocateType, Record}
import com.bonc.lla.LLA
import com.bonc.location.LocType
import com.bonc.trace.{GTrace, Trace}
import com.bonc.user_locate.ReadParam
import com.bonc.xyz.XYZ
import org.apache.commons.lang.math.NumberUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * 用MR数据生成50*50栅格级别的用户轨迹数据
 *
 * @author opslos
 * @date 2021/1/13 10:48:36
 */
object MRTrace_hour {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("Warn").setLevel(Level.WARN)
    Logger.getLogger("Error").setLevel(Level.ERROR)

    if (args.length < 2) {
      println("args must be at least 2,xml&procDate")
      System.exit(1)
    }

    val conf_xml: String = args(0)
    val procDate: String = args(1)
    val procHour: String = args(2)

    val sconf: SparkConf = new SparkConf().setAppName("mrTrace")
    // 读取配置文件
    ReadParam.readXML(sconf, conf_xml)

    if (sconf.get("local").toBoolean) {
      sconf.setMaster("local[*]")
    }

    val sparkContext = new SparkContext(sconf)

    val hiveContext = new HiveContext(sparkContext)


    // 配置参数
    val readLib: String = sconf.get("hive_readLib")

    val readTable: String = sconf.get("hive_readTable")

    val insertLib: String = sconf.get("hive_insertLib")

    val insertTable: String = sconf.get("hive_insertTable")

    val L0: Double = NumberUtils.toDouble(sconf.get("L0"))

    val gridScale: Double = NumberUtils.toInt(sconf.get("gridScale"))

    val readSQL = s"select imsi,stime,maincellid,longitude,latitude,loctype from ${readLib}.${readTable} where day_id=${procDate} and hour=${procHour}"

    val inputRDD: RDD[(String, String, String, String, String)] = hiveContext.sql(readSQL).rdd.filter(x => {
      x.get(5).equals("5") || x.get(5).equals("6")
    }).map(x => {
      val imsi: String = x.get(0).toString
      val stime: String = x.get(1).toString
      val maincellid: String = x.get(2).toString
      val longitude: String = x.get(3).toString
      val latitude: String = x.get(4).toString
      (imsi, stime, maincellid, longitude, latitude)
    })

    val makeRecordRDD: RDD[Record] = inputRDD.map(x => {
      val record = new Record
      val longitude: Double = NumberUtils.toDouble(x._4)
      val latitude: Double = NumberUtils.toDouble(x._5)
      val lla = new LLA(longitude, latitude)
      record.lLA = lla
      record.sTime = NumberUtils.toLong(x._2)
      record.eTime = NumberUtils.toLong(x._2)
      record.imsi = x._1
      record.locateType = LocateType.MR
      record.eci = NumberUtils.toLong(x._3)
      // 获取50*50的栅格编号
      val xyz: XYZ = lla.toXYZ(L0)
      val xyzX: Int = (xyz.x / gridScale).toInt
      val xyzY: Int = (xyz.y / gridScale).toInt
      val grid: String = xyzX + "-" + xyzY
      record.gridCode = grid
      record
    })

    val makeTraceRDD: RDD[GTrace] = makeRecordRDD.groupBy(_.imsi).map(x => {
      val trace = new GTrace(LocType.GRID)
      val records: List[Record] = x._2.toList.sortBy(_.sTime)
      trace.create(x._1, records)
      trace.date = procDate
      trace.getAllEciList(x._2.toList)
      trace
    })


//    val resultList: List[GTrace] = makeTraceRDD.collect().take(10000).toList
//
//    for (elem <- resultList.take(20)) {
//      println(elem.date+"\u0001"+elem.imsi+"\u0001"+elem.wheres.length)
//      elem.wheres.foreach(x => {
//        println(x.toString)
//      })
//    }

    val resultRDD: RDD[(String, String, String, String, String, String, String, String, String, String)] = makeTraceRDD.flatMap(x => {
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      // 数据日期,手机号,栅格ID,IMEI,IMSI,开始时间,停留时间,结束时间,栅格内ECI列表,轨迹数据源类型
      val tuples = new ListBuffer[(String, String, String, String, String, String, String, String, String, String)]

      x.wheres.foreach(y => {
        val sDate: String = dateFormat.format(new Date(y.sTime))
        val eDate: String = dateFormat.format(new Date(y.eTime))
        val listBuffer: ListBuffer[Long] = x.ecis(y.code)
        val stringBuilder = new StringBuilder
        for (elem <- listBuffer.distinct) {
          stringBuilder.append(elem + "#")
        }
        val xyz = new XYZ(NumberUtils.toDouble(y.code.split("-")(0)) * gridScale, NumberUtils.toDouble(y.code.split("-")(1)) * gridScale)
        val lla: LLA = xyz.toLLA(L0)
        val lonLat: String = lla.longitude + "," + lla.latitude
        val allECI: String = stringBuilder.toString()
        tuples.append(("", y.code, "", x.imsi, lonLat, sDate, ((y.eTime - y.sTime) / 1000).toString, eDate, allECI, "MR"))
      })

      tuples
    })

    val rowRDD: RDD[Row] = resultRDD.map(x => Row(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9, x._10))
    val schema = "calling_no,grid,calling_imei,calling_imsi,lonLat,start_time,stay_time,end_time,ecis,source"
    val structType: StructType = StructType(schema.split(",").map(x => StructField(x, StringType, true)))
    val dataFrame: DataFrame = hiveContext.createDataFrame(rowRDD, structType)
    dataFrame.createOrReplaceTempView("tmpTable")
    val insertSQL: String = s"insert overwrite table ${insertLib}.${insertTable} partition(prodate='" + procDate + "',prohour='"+procHour+"') " +
      "select calling_no,grid,calling_imei,calling_imsi,lonLat,start_time,stay_time,end_time,ecis,source from tmpTable"
    println(insertSQL)
    hiveContext.sql(insertSQL)
    println("InOutFlowAnalyze-----"+procDate+"-----"+procHour+"-----SUCCESS!")
    sparkContext.stop()

  }
}


































