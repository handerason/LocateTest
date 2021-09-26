package com.bonc.ServerTestRebuild

import java.io.BufferedReader

import com.bonc.lla.LLA
import com.bonc.location.LocType
import com.bonc.trace.{Trace, Where}
import com.bonc.user_locate.ReadParam
import com.bonc.user_locate.utils.FileUtil
import org.apache.commons.lang.math.NumberUtils
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}

/**
 * 通过话单和融合定位的融合轨迹生成省级的用户OD
 *
 * @author opslos
 * @date 2021/4/29 16:53:14
 */
object provinceBetweenOD {
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

    //    val str_hour = args.length match {
    //      case 1 => null
    //      case 2 => args(1)
    //      case _ => null
    //    }

    val conf = new SparkConf().setAppName("mergeTraceCellFlow")
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

    //读取hdfs：.dat
    val dataTextFile: BufferedReader = FileUtil.getBufferedReader(new Path(conf.get("datPath") + str_date + "_00_001.dat"))
    var line: String = ""
    line = dataTextFile.readLine()
    val list = new mutable.HashMap[String, String]()
    while (line != null) {
      if (line.split("\\|", -1)(5).equals("86"))
      //（电话号码，日数据分区+开始时间+结束时间+国家编码+下一开始时间+时间间隔+来源省份代码+来源省份名称+到访省份代码+到访省份名称）
        list.put("86" + line.split("\\|", -1)(1), line.split("\\|", -1)(0) + "\t" + line.split("\\|", -1)(3) + "\t" + line.split("\\|", -1)(4) + "\t" + line.split("\\|", -1)(5) + "\t" + line.split("\\|", -1)(10) + "\t" + line.split("\\|", -1)(11) + "\t" + line.split("\\|", -1)(14) + "\t" + line.split("\\|", -1)(15) + "\t" + line.split("\\|", -1)(8) + "\t" + line.split("\\|", -1)(16))
      line = dataTextFile.readLine()
    }
    dataTextFile.close()
    println("list" + list.size)
    val ProvinceODBroad: Broadcast[mutable.HashMap[String, String]] = sparkContext.broadcast(list)

    // 读取国信漫出用户详单表（表1）数据
    //（当前省份代码，来源省份代码，IMSI，IMEI，电话号码，业务开始时间，业务结束时间，基站经度，基站纬度，处理状态，ECI，流程类型，home）
    val readSQL = s"select local_prov_no,home_prov_id,imsi,imei,msisdn,busi_start_time,busi_end_time,start_longitude,start_latitude,prcs_sts,ci,procedure_type,stat_hour from ${inputMergeTraceDataBase}.${inputMergeTraceTable} where stat_hour like concat ('${str_date}','%') "
    val readInRDD: RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String)] = hiveContext.sql(readSQL).rdd.map(x => {
      var local_prov_no: String = ""
      if (!x.isNullAt(0) && !x.get(0).toString.isEmpty) {
        local_prov_no = x.get(0).toString
      }
      var home_prov_id: String = ""
      if (!x.isNullAt(1) && !x.get(1).toString.isEmpty) {
        home_prov_id = x.get(1).toString
      }
      var imsi: String = ""
      if (!x.isNullAt(2) && !x.get(2).toString.isEmpty) {
        imsi = x.get(2).toString
      }
      var imei: String = ""
      if (!x.isNullAt(3) && !x.get(3).toString.isEmpty) {
        imei = x.get(3).toString
      }
      var msisdn: String = ""
      if (!x.isNullAt(4) && !x.get(4).toString.isEmpty) {
        msisdn = x.get(4).toString
      }
      var busi_start_time: String = ""
      if (!x.isNullAt(5) && !x.get(5).toString.isEmpty) {
        busi_start_time = x.get(5).toString
      }
      var busi_end_time: String = ""
      if (!x.isNullAt(6) && !x.get(6).toString.isEmpty) {
        busi_end_time = x.get(6).toString
      }
      var start_longitude: String = ""
      if (!x.isNullAt(7) && !x.get(7).toString.isEmpty) {
        start_longitude = x.get(7).toString
      }
      var start_latitude: String = ""
      if (!x.isNullAt(8) && !x.get(8).toString.isEmpty) {
        start_latitude = x.get(8).toString
      }
      var prcs_sts: String = ""
      if (!x.isNullAt(9) && !x.get(9).toString.isEmpty) {
        prcs_sts = x.get(9).toString
      }
      var eci: String = ""
      if (!x.isNullAt(10) && !x.get(10).toString.isEmpty) {
        eci = x.get(10).toString
      }
      var procedure_type: String = ""
      if (!x.isNullAt(11) && !x.get(11).toString.isEmpty) {
        procedure_type = x.get(11).toString
      }
      var stat_hour: String = ""
      if (!x.isNullAt(12) && !x.get(12).toString.isEmpty) {
        stat_hour = x.get(12).toString
      }
      (local_prov_no, home_prov_id, imsi, imei, msisdn, busi_start_time, busi_end_time, start_longitude, start_latitude, prcs_sts, eci, procedure_type, stat_hour)
    })
    println("readInRDD:" + readInRDD.count())

    //（当前省份代码，来源省份代码，IMSI，IMEI，电话号码，业务开始时间，业务结束时间，基站经度，基站纬度，处理状态，ECI，流程类型，home）
    //    （电话号码，日数据分区+开始时间+结束时间+国家编码+下一开始时间+时间间隔+来源省份代码+来源省份名称+到访省份代码+到访省份名称）
    val traceRDD: RDD[Trace] = readInRDD.map(x => {
      val provinceODBroad: mutable.HashMap[String, String] = ProvinceODBroad.value
      val where = new Where()
      if (x._11.nonEmpty) {
        where.code = x._11
      }
      if (x._8.nonEmpty && x._9.nonEmpty) {
        where.lLA = new LLA(NumberUtils.toDouble(x._8), NumberUtils.toDouble(x._9))
      } else {
        where.lLA = new LLA(0D, 0D)
      }
      //省份映射*********
      //到访省份代码+到访省份名称
      if (provinceODBroad.contains(x._5)) {
        where.provinceCity = provinceODBroad(x._5).split("\t", -1)(8) + "\u0001" + provinceODBroad(x._5).split("\t", -1)(9)
      }
      //        开始时间和结束时间映射hfds上的那个文件中的开始时间和结束时间
      if (provinceODBroad.contains(x._5)) {
        where.sTime = NumberUtils.toLong(provinceODBroad(x._5).split("\t", -1)(1))
      }
      if (provinceODBroad.contains(x._5)) {
        where.eTime = NumberUtils.toLong(provinceODBroad(x._5).split("\t", -1)(2))
      }
      (x._3 + "\t" + x._5, where)

    }).groupByKey()
      .map(x => {
        val sortWheres: List[Where] = x._2.toList.sortBy(_.sTime)
        for (elem <- sortWheres) {
          if (sortWheres.indexOf(elem) != sortWheres.length - 1) {
            if (elem.eTime > sortWheres(sortWheres.indexOf(elem) + 1).sTime) {
              elem.eTime = Math.min(elem.eTime, sortWheres(sortWheres.indexOf(elem) + 1).sTime)
              sortWheres(sortWheres.indexOf(elem) + 1).sTime = Math.min(elem.eTime, sortWheres(sortWheres.indexOf(elem) + 1).sTime)
            }
          }
        }
        (x._1, sortWheres)
      })
      .map((x: (String, List[Where])) => {
        val trace = new Trace(LocType.SECTOR)
        x._2.foreach(y => {
          trace.loadAsLast(y)
        })
        trace.imsi = x._1.split("\t", -1)(0)
        trace.pohoneNum = x._1.split("\t", -1)(1)
        trace.date = str_date
        trace
      })
    println("traceRDD:" + traceRDD.count())

    //    val anotherRDD: RDD[(String, ListBuffer[Where],String)] = traceRDD.map(x => {
    //      var sortWheres = new ListBuffer[Where]()
    //        sortWheres.append(x._2)
    //      for (elem <- sortWheres) {
    //        if (sortWheres.indexOf(elem) != sortWheres.length - 1) {
    //          if (elem.eTime > sortWheres(sortWheres.indexOf(elem) + 1).sTime) {
    //            elem.eTime = Math.min(elem.eTime, sortWheres(sortWheres.indexOf(elem) + 1).sTime)
    //            sortWheres(sortWheres.indexOf(elem) + 1).sTime = Math.min(elem.eTime, sortWheres(sortWheres.indexOf(elem) + 1).sTime)
    //          }
    //        }
    //      }
    //      (x._1, sortWheres,x._3)
    //    })
    //    println("anotherRDD:" + anotherRDD.count())

    //    val traceRDD: RDD[Trace] = whereRDD.map(x => {
    //      val trace = new Trace(LocType.SECTOR)
    //      trace.loadAsLast(x._2)
    //      trace.imsi = x._1
    //      trace.pohoneNum = x._3
    //      trace.date = str_date
    //      trace
    //    })
    //    println("traceRDD" + traceRDD.count())


    //    日期#
    //    IMSI#
    //    手机号码#
    //    省O点基站小区标识
    //    O点基站归属省编码#
    //    O点基站归属省名称#
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
      val provinceODBroad: mutable.HashMap[String, String] = ProvinceODBroad.value
      var thisProvince = ""
      var lastProvince = ""
      //到访省份代码+到访省份名称
      for (elem <- x.wheres) {
        breakable {
          thisProvince = elem.provinceCity.split("\u0001", -1)(0)
          if (thisProvince.isEmpty && lastProvince.isEmpty) {
            thisProvince = elem.provinceCity.split("\u0001", -1)(0)
            lastProvince = elem.provinceCity.split("\u0001", -1)(0)
            break()
          }
          if (!thisProvince.equals(lastProvince)) {
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

            all20list.append((x.date, x.imsi, x.pohoneNum, lastWhere.code, oprovinceNum, oprovinceName, oprovinceScene, lastWhere.sTime.toString, lastWhere.eTime.toString, thisWhere.code, dprovinceNum, dprovinceName, dprovinceScene, thisWhere.sTime.toString, thisWhere.eTime.toString, distanceKM.toString, costHour, speed, "", ""))

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
    val insertSQL: String = s"insert overwrite table ${outputDateBase}.${outputTable} partition(prodate='${str_date}') " +
      "select date_day,imsi,msisdn,O_cellID,O_province_code,O_province_name,O_cell_state,O_in_time,O_out_time,D_cellID,D_province_code,D_province_name,D_cell_state,D_in_time,D_out_time,OD_province_distance,OD_province_duration,OD_province_velocity,OD_province_type,home_province from tmpTable"
    println(insertSQL)
    hiveContext.sql(insertSQL)


    sparkContext.stop()
    println("success!————省际OD完成" + str_date)

  }
}

























