package com.bonc.ServerTestRebuild

import java.io.BufferedReader
import java.text.SimpleDateFormat
import java.util.TimeZone

import com.bonc.data.{LocateType, Record}
import com.bonc.lla.LLA
import com.bonc.location.LocType
import com.bonc.trace.Trace
import com.bonc.user_locate.ReadParam
import com.bonc.user_locate.utils.{FileUtil, getGeoHash}
import org.apache.commons.lang.math.NumberUtils
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * @Author: whzHander
 * @Date: 2021/8/2 17:44
 * @Description:
 */
object mergeTraceCellFlowTest {
  def main(args: Array[String]): Unit = {

    //日志级别设置
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("com").setLevel(Level.WARN)

    if (args.length < 1) {
      println("args must be at least 1")
      System.exit(1)
    }
    val configPath = args(0)
    val str_date = args.length match {
      case 1 => null
      case 2 => args(1)
      case _ => args(1)
    }

    val conf = new SparkConf().setAppName("mergeTraceCellFlow")
    ReadParam.readXML(conf, configPath)

    val isLocal = conf.get("local")
    if ("1".equals(isLocal)) {
      conf.setMaster("local[*]")
    }

    val inputDateBase: String = conf.get("inputDataBase")
    val inputTale: String = conf.get("inputTale")
    val outputDateBase: String = conf.get("outputDataBase")
    val outputTable: String = conf.get("outputTable")

    val sc = new SparkContext(conf) // 创建SparkContext
    val hiveContext = new HiveContext(sc)

    // 将区县信息做成HashMap的结构（eci，区县）
    // 加载4G工参,格式：(eci,区县)，到时候这个参数处理一下
    //    val G4Reader: BufferedReader = FileUtil.getBufferedReader(new Path(conf.get("G4Path")))
    //    val G4List = new mutable.HashMap[String, String]()
    //    var G4: String = ""
    //    G4 = G4Reader.readLine()
    //    while (G4 != null) {
    //      if (!G4.contains("-")) {
    //                                        // 城市+“_”+区县
    //        G4List.put(G4.split("\t")(0), G4.split("\t")(6)+"_"+G4.split("\t")(7))
    //      }
    //      G4 = G4Reader.readLine()
    //    }
    //    G4Reader.close()
    //    println("G4List" + G4List.size)
    //    val G4Broad: Broadcast[mutable.HashMap[String, String]] = sc.broadcast(G4List)
    //
    //    val G2Reader: BufferedReader = FileUtil.getBufferedReader(new Path(conf.get("G2Path")))
    //    val G2List = new mutable.HashMap[String, String]()
    //    var G2: String = ""
    //    G2 = G2Reader.readLine()
    //    while (G2 != null) {
    //      if (!G2.contains("-")) {
    //        G2List.put(G2.split("\t")(0), G2.split("\t")(6)+"_"+G2.split("\t")(7))
    //      }
    //      G2 = G2Reader.readLine()
    //    }
    //    G2Reader.close()
    //    println("G2List" + G2List.size)
    //    val G2Broad: Broadcast[mutable.HashMap[String, String]] = sc.broadcast(G2List)

    val gcReader: BufferedReader = FileUtil.getBufferedReader(new Path(conf.get("G4Path")))
    val G2List = new mutable.HashMap[String, String]()
    val G4List = new mutable.HashMap[String, String]()
    var gc: String = ""
    gc = gcReader.readLine()
    while (gc != null) {
      if (!gc.contains("-")) {
        // 城市+“_”+区县
        if (gc.split("\\|").length >= 5) {
          if (gc.split("\\|")(2).contains("_")) {
            G2List.put(gc.split("\\|")(2), gc.split("\\|")(3) + "_" + gc.split("\\|")(5))
          } else {
            G4List.put(gc.split("\\|")(2), gc.split("\\|")(3) + "_" + gc.split("\\|")(5))
          }
        }
      }
      gc = gcReader.readLine()
    }
    gcReader.close()
    println("G4List" + G4List.size)
    println("G2List" + G2List.size)
    val G4Broad: Broadcast[mutable.HashMap[String, String]] = sc.broadcast(G4List)
    val G2Broad: Broadcast[mutable.HashMap[String, String]] = sc.broadcast(G2List)


    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    format.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))

    // 读取融合定位数据
    val readSQL = s"select imsi,loctype,stime,maincellid,longitude,latitude from ${inputDateBase}.${inputTale} where day_id='${str_date}'"
    val inputRDD: RDD[(String, String, String, String, String, String)] = hiveContext.sql(readSQL)
      .rdd
      .map((x: Row) => {
        var imsi: String = ""
        if (!x.isNullAt(0) && !x.get(0).toString.isEmpty) {
          imsi = x.get(0).toString
        }
        var loctype: String = ""
        if (!x.isNullAt(1) && !x.get(1).toString.isEmpty) {
          loctype = x.get(1).toString
        }
        var stime: String = ""
        if (!x.isNullAt(2) && !x.get(2).toString.isEmpty) {
          stime = x.get(2).toString
        }
        var maincellid: String = ""
        if (!x.isNullAt(3) && !x.get(3).toString.isEmpty) {
          maincellid = x.get(3).toString
        }
        var longitude: String = ""
        if (!x.isNullAt(4) && !x.get(4).toString.isEmpty) {
          longitude = x.get(4).toString
        }
        var latitude: String = ""
        if (!x.isNullAt(5) && !x.get(5).toString.isEmpty) {
          latitude = x.get(5).toString
        }
        (imsi, loctype, stime, maincellid, longitude, latitude)
      })
    println("inputRDD:" + inputRDD.count())

    val recordRDD: RDD[(String, Record)] = inputRDD.map((x: (String, String, String, String, String, String)) => {
      val record = new Record
      // imsi
      record.imsi = x._1
      // locType
      if ("2".equals(x._2)) {
        record.locateType = LocateType.MDT
      } else if ("5".equals(x._2) || "6".equals(x._2)) {
        record.locateType = LocateType.MR
      } else if ("7".equals(x._2) || "8".equals(x._2)) {
        record.locateType = LocateType.XDR
      }
      // rat!!!
      if (x._4.contains("_")) {
        record.rat = 2
      } else {
        record.rat = 6
      }
      // eci
      if (record.rat == 2 && x._4.split("_").length == 2) {
        record.eci = NumberUtils.toLong(x._4.split("_")(0)) * 100000L + NumberUtils.toLong(x._4.split("_")(1))
      } else {
        record.eci = NumberUtils.toLong(x._4)
      }
      // LLA
      record.lLA = new LLA(NumberUtils.toDouble(x._5), NumberUtils.toDouble(x._6))
      // stime
      record.sTime = NumberUtils.toLong(x._3)
      (x._1, record)
    })
    println("recordRDD:" + recordRDD.count())

    val traceRDD: RDD[Trace] = recordRDD
      .groupByKey()
      .map((x: (String, Iterable[Record])) => {
        val trace = new Trace(LocType.SECTOR)
        val records: List[Record] = x._2.toList.sortBy(_.sTime)
        for (elem <- records) {
          if (records.indexOf(elem) != records.length - 2) {
            if (records(records.indexOf(elem) + 1).eci != elem.eci) {
              if (records(records.indexOf(elem)+2).eci!=records(records.indexOf(elem)+1).eci) {
                records(records.indexOf(elem)+2).eci=elem.eci
              }
            }
          }
        }
        trace.create(x._1, records)
        trace
      })
    println("traceRDD:" + traceRDD.count())


    val resultRDD: RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String, String)] = traceRDD.flatMap(x => {
      val G2B: mutable.HashMap[String, String] = G2Broad.value
      val G4B: mutable.HashMap[String, String] = G4Broad.value
      val listBuffer = new ListBuffer[(String, String, String, String, String, String, String, String, String, String, String, String, String, String)]
      x.wheres.foreach(y => {
        var district = ""
        var city = ""
        if (y.rat == 2) {
          val dc = G2B.getOrElse(y.code, "")
          if (dc.nonEmpty) {
            city = dc.split("_")(0)
            district = dc.split("_")(1)
          }
        } else {
          val dc = G4B.getOrElse(y.code, "")
          if (dc.nonEmpty) {
            city = dc.split("_")(0)
            district = dc.split("_")(1)
          }
        }
        val geoCode: String = getGeoHash.encode(y.lLA.longitude, y.lLA.latitude)
        // imsi,msisdn,imei,lac_cell,rat,longitude,latitude,geohash,country_code,city_code,county_code,enter_time,leave_time,source
        listBuffer.append((x.imsi, "", "", y.code, y.rat.toString, y.lLA.longitude.toString, y.lLA.latitude.toString, geoCode, "CN", city, district, y.sTime.toString, y.eTime.toString, y.source))
      })
      listBuffer
    })
    println("resultRDD:" + resultRDD.count())

    val resultRow: RDD[Row] = resultRDD.map(x => {
      Row(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9, x._10, x._11, x._12, x._13, x._14)
    })

    val schema = "imsi,msisdn,imei,lac_cell,rat,longitude,latitude,geohash,country_code,city_code,county_code,enter_time,leave_time,source"
    val structType: StructType = StructType(schema.split(",").map(x => StructField(x, StringType, true)))
    val dataFrame: DataFrame = hiveContext.createDataFrame(resultRow, structType)
    dataFrame.createOrReplaceTempView("tmpTable")
    val insertSQL: String = s"insert overwrite table ${outputDateBase}.${outputTable} partition(day_id='" + str_date + "') " +
      "select imsi,msisdn,imei,lac_cell,rat,longitude,latitude,geohash,country_code,city_code,county_code,enter_time,leave_time,source from tmpTable"
    println(insertSQL)
    hiveContext.sql(insertSQL)

    sc.stop()
    println("success!————基站出入模型" + str_date)

  }
}
