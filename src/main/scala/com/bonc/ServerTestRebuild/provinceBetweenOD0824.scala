package com.bonc.ServerTestRebuild

import java.io.BufferedReader
import java.text.SimpleDateFormat
import java.util

import com.bonc.data.{LocateType, Record}
import com.bonc.lla.LLA
import com.bonc.location.LocType
import com.bonc.trace.{Trace, Where}
import com.bonc.user_locate.ReadParam
import com.bonc.user_locate.utils.FileUtil
import com.bonc.utils.TimeStampUtils
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
import scala.util.control.Breaks.{break, breakable}

/**
 * @Author: whzHander
 * @Date: 2021/7/22 10:59
 * @Description:
 * This code sucks, you know it and I know it.
 * Move on and call me an idiot later.
 */
object provinceBetweenOD0824 {
  def main(args: Array[String]): Unit = {

    // 利用话单和融合定位轨迹融合的数据来做
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

    //        val str_hour = args.length match {
    //          case 1 => null
    //          case 2 => args(1)
    //          case _ => null
    //        }

    val conf = new SparkConf().setAppName("provinceBetweenODTest")
    ReadParam.readXML(conf, configPath)

    val sparkContext = new SparkContext(conf)
    val hiveContext = new HiveContext(sparkContext)

    val isLocal = conf.get("local")
    if ("1".equals(isLocal)) {
      conf.setMaster("local[*]")
    }

    val inputMergeTraceDataBase: String = conf.get("inputMergeTraceDataBase")
    val inputMergeTraceTable: String = conf.get("inputMergeTraceTable")
    val outputDateBase: String = conf.get("outputDataBase")
    val outputTable: String = conf.get("outputTable")

    //读取漫出数据
    //    （手机号，开始时间，结束时间，下一开始时间，时间间隔，来源省份代码，来源省份名称，到访省份代码，到访省份名称）
    val odValueRDD: RDD[(String, String)] = sparkContext.textFile(conf.get("datPath") + str_date + "_00_001.dat").filter((x: String) => x.split("\\|", -1)(5).equals("86") && !x.split("\\|", -1)(8).isEmpty).map((line: String) => {

      val calling_no: String = line.split("\\|", -1)(1)
      val start_time: String = line.split("\\|", -1)(3)
      val end_time: String = line.split("\\|", -1)(4)
      val prov_code: String = line.split("\\|", -1)(8)
      val next_start_time: String = line.split("\\|", -1)(10)
      val prov_name: String = line.split("\\|", -1)(16)
      (calling_no, calling_no + "\t" + start_time + "\t" + end_time + "\t" + next_start_time + "\t" + prov_code + "\t" + prov_name)
    })
    println("odValueRDD" + odValueRDD.count())
    odValueRDD.take(5).foreach {
      println
    }

    val map = new mutable.HashMap[String, Iterable[String]]()
    //    漫出数据较少，转换成广播变量
    val tuples: Array[(String, Iterable[String])] = odValueRDD.groupByKey().collect()
    tuples.map((x: (String, Iterable[String])) => {
      map.put(x._1, x._2)
    })
    //    漫出数据
    val roam: Broadcast[mutable.HashMap[String, Iterable[String]]] = sparkContext.broadcast(map)

    // 读取信令数据
    //（归属省份代码，IMSI，电话号码，业务开始时间，ECI，home）
    val readSQL = s"select home_prov_id,imsi,msisdn,busi_start_time,ci,start_longitude,start_latitude,stat_hour from ${inputMergeTraceDataBase}.${inputMergeTraceTable} where stat_hour like concat ('${str_date}','%') and msisdn like concat ('86','%') "
    val readInRDD: RDD[(String, String, String, String, String, String, String, String)] = hiveContext.sql(readSQL).rdd.map((x: Row) => {
      var home_prov_id: String = ""
      if (!x.isNullAt(0) && !x.get(0).toString.isEmpty) {
        home_prov_id = x.get(0).toString
      }
      var imsi: String = ""
      if (!x.isNullAt(1) && !x.get(1).toString.isEmpty) {
        imsi = x.get(1).toString
      }
      var msisdn: String = ""
      if (!x.isNullAt(2) && !x.get(2).toString.isEmpty) {
        msisdn = x.get(2).toString.substring(2, 13)
      }
      var time: String = ""
      var busi_start_time: String = ""
      if (!x.isNullAt(3) && !x.get(3).toString.isEmpty) {
        busi_start_time = x.get(3).toString
        time = TimeStampUtils.tranTimeToLong(busi_start_time).toString
      }
      var eci: String = ""
      if (!x.isNullAt(4) && !x.get(4).toString.isEmpty) {
        eci = x.get(4).toString
      }
      var start_longitude: String = ""
      if (!x.isNullAt(5) && !x.get(5).toString.isEmpty) {
        start_longitude = x.get(5).toString
      }
      var start_latitude: String = ""
      if (!x.isNullAt(6) && !x.get(6).toString.isEmpty) {
        start_latitude = x.get(6).toString
      }
      (msisdn, home_prov_id, imsi, busi_start_time, eci, time, start_longitude, start_latitude)
    })
    println("readInRDD:" + readInRDD.count())
    readInRDD.take(5).foreach {
      println
    }

    val traceRDD: RDD[Trace] = readInRDD.filter((x: (String, String, String, String, String, String, String, String)) =>{
      val roamValue: mutable.HashMap[String, Iterable[String]] = roam.value
      roamValue.contains(x._1)
    }).map((x: (String, String, String, String, String, String, String, String)) => {
      val record = new Record
      record.imsi = x._3
      record.locateType = LocateType.XDR
      if (x._5.contains("_")) {
        record.rat = 2
      } else {
        record.rat = 6
      }
      // eci
      if (record.rat == 2 && x._5.split("_").length == 2) {
        record.eci = NumberUtils.toLong(x._5.split("_")(0)) * 100000L + NumberUtils.toLong(x._5.split("_")(1))
      } else {
        record.eci = NumberUtils.toLong(x._5)
      }
      // LLA
      record.lLA = new LLA(NumberUtils.toDouble(x._7), NumberUtils.toDouble(x._8))
      // stime
      record.sTime = TimeStampUtils.tranTimeToLong(x._4)
      (x._1 + "\t" + x._2 + "\t" + x._3, record)
    }).groupByKey()
      .map((x: (String, Iterable[Record])) => {
        val trace = new Trace(LocType.SECTOR)
        val records: List[Record] = x._2.toList.sortBy((_: Record).sTime)
        trace.create(x._1.split("\t",-1)(2), records)
        trace.pohoneNum=x._1.split("\t",-1)(0)+"\t"+x._1.split("\t",-1)(1)
        trace
      })

    println("traceRDD:" + traceRDD.count())
    traceRDD.take(5).foreach {
      println
    }

    traceRDD.map((x: Trace) =>{
      val roamValue: mutable.HashMap[String, Iterable[String]] = roam.value
      val list = new ListBuffer[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)]
      roamValue
    })

    //    日期#
    //    IMSI#
    //    手机号码#
    //    省O点基站小区标识
    //    O点基站归属省编码#
    //    O点基站归属省名称# .
    //    O点基站所属场景
    //    到达省O点基站时间#
    //    省O点出发时间#
    //    D点基站编号
    //    D点基站归属省编码#
    //    D点基站归属省名称#
    //    D点基站所属场景
    //    D点基站到达时间#
    //    D点省最后离开基站时间#
    //    省间OD距离（公里）
    //    省间OD时长（小时）
    //    省间OD速度
    //    省间OD出行方式
    //    手机号码归属省#
    val resultRDD: RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)] = traceRDD.flatMap(x => {
      val all20list = new ListBuffer[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)]
      var thisProvince = ""
      var lastProvince = ""
      //      到访省份代码 到访省份名称 归属省份代码
      for (elem <- x.wheres) {
        breakable {
          thisProvince = elem.provinceCity.split("\u0001", -1)(0)
          if (thisProvince.isEmpty && lastProvince.isEmpty) {
            thisProvince = elem.provinceCity.split("\u0001", -1)(0)
            lastProvince = elem.provinceCity.split("\u0001", -1)(0)
            break()
          }
          if (!thisProvince.equals(lastProvince) && !thisProvince.isEmpty) {
            val thisWhere: Where = elem
            var lastWhere: Where = elem
            if (x.wheres.indexOf(elem) != x.wheres.length - 1 && x.wheres.indexOf(elem) != 0 && x.wheres.indexOf(elem) != 1) {
              lastWhere = x.wheres(x.wheres.indexOf(elem) - 1)
            }
            // 进行处理
            // 日期,IMSI,手机号码,地市O 点基站小区标识,O 点基站归属地市编码,O 点基站归属地市名称,O 点基站所属场景,到达地市O点基站时间,地市O 点出发时间,D 点基站编号,D点基站归属地市编码,D点基站归属地市名称,D 点基站所属场景,D 点基站到达时间,D点地市最后离开基站时间,地市间OD距离（公里）,地市间OD时长（小时）,地市间OD速度,地市间OD出行方式,手机号码归属省,
            //    O点基站归属省编码#
            //    O点基站归属省名称#
            //    O点基站所属场景
            var oprovinceName: String = ""
            if (lastWhere.provinceCity.split("\u0001", -1).isDefinedAt(1)) {
              oprovinceName = lastWhere.provinceCity.split("\u0001", -1)(1)
            }
            var oprovinceNum: String = ""
            if (lastWhere.provinceCity.split("\u0001", -1).isDefinedAt(0)) {
              oprovinceNum = lastWhere.provinceCity.split("\u0001", -1)(0)
            }
            val oprovinceScene: String = ""

            //    D点基站归属省编码#
            //    D点基站归属省名称#
            //    D点基站所属场景
            var dprovinceName: String = ""
            if (thisWhere.provinceCity.split("\u0001", -1).isDefinedAt(1)) {
              dprovinceName = thisWhere.provinceCity.split("\u0001", -1)(1)
            }
            var dprovinceNum: String = ""
            if (thisWhere.provinceCity.split("\u0001", -1).isDefinedAt(0)) {
              dprovinceNum = thisWhere.provinceCity.split("\u0001", -1)(0)
            }

            val dprovinceScene: String = ""


            val distanceKM: Double = 0D
            val costSeconds: Double = (thisWhere.sTime - lastWhere.eTime) / 1000

            val speed: String = "0"

            val costHour: String = (costSeconds / 3600).formatted("%.2f")
            val phoneNum: String = x.pohoneNum.split("\t", -1)(0)
            val home_province: String = x.pohoneNum.split("\t", -1)(1)

            all20list.append((x.date, x.imsi, phoneNum, lastWhere.code, oprovinceNum, oprovinceName, oprovinceScene, TimeStampUtils.longToTranTime(lastWhere.sTime), TimeStampUtils.longToTranTime(lastWhere.eTime), thisWhere.code, dprovinceNum, dprovinceName, dprovinceScene, TimeStampUtils.longToTranTime(thisWhere.sTime), TimeStampUtils.longToTranTime(thisWhere.eTime), distanceKM.toString, costHour, speed, "", home_province))

            lastProvince = thisProvince
          }
        }

      }
      all20list
    })
    println("resultRDD:" + resultRDD.count())

    val rowRDD: RDD[Row] = resultRDD.map(x => Row(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9, x._10, x._11, x._12, x._13, x._14, x._15, x._16, x._17, x._18, x._19, x._20))

    val schema = "date_day,imsi,msisdn,O_cellID,O_province_code,O_province_name,O_cell_state,O_in_time,O_out_time,D_cellID,D_province_code,D_province_name,D_cell_state,D_in_time,D_out_time,OD_province_distance,OD_province_duration,OD_province_velocity,OD_province_type,home_province"
    val structType: StructType = StructType(schema.split(",").map(x => StructField(x, StringType, true)))
    val dataFrame: DataFrame = hiveContext.createDataFrame(rowRDD, structType)
    dataFrame.createOrReplaceTempView("tmpTable")
    val insertSQL: String = s"insert overwrite table ${outputDateBase}.${outputTable} partition(prodate='${str_date}',stat_hour='${str_date}') " +
      "select date_day,imsi,msisdn,O_cellID,O_province_code,O_province_name,O_cell_state,O_in_time,O_out_time,D_cellID,D_province_code,D_province_name,D_cell_state,D_in_time,D_out_time,OD_province_distance,OD_province_duration,OD_province_velocity,OD_province_type,home_province from tmpTable"
    println(insertSQL)
    hiveContext.sql(insertSQL)


    sparkContext.stop()
    println("success!————省际OD完成" + str_date)

  }
}
