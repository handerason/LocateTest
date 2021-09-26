package com.bonc.ServerTestRebuild

import java.io.BufferedReader
import java.text.SimpleDateFormat

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
 * @Date: 2021/8/13 9:05
 * @Description:
 * This code sucks, you know it and I know it.
 * Move on and call me an idiot later.
 * If this code works, it was written by Wang Hengze.
 * If not, I don't know who wrote it.
 */
object countryBetweenODTest {
  def main(args: Array[String]): Unit = {

    // 利用话单和融合定位轨迹融合的数据来做
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("com").setLevel(Level.WARN)

    if (args.length < 1) {
      println("args must be at least 1")
      System.exit(1)
    }
    val configPath: String = args(0)
    val str_date: String = args.length match {
      case 1 => null
      case 2 => args(1)
      case _ => args(1)
    }

    //        val str_hour = args.length match {
    //          case 1 => null
    //          case 2 => args(1)
    //          case _ => null
    //        }

    val conf: SparkConf = new SparkConf().setAppName("countryBetweenODTest")
    ReadParam.readXML(conf, configPath)

    val sparkContext = new SparkContext(conf)
    val hiveContext = new HiveContext(sparkContext)

    val isLocal: String = conf.get("local")
    if ("1".equals(isLocal)) {
      conf.setMaster("local[*]")
    }

    val inputMergeTraceDataBase: String = conf.get("inputMergeTraceDataBase")
    val inputMergeTraceTable: String = conf.get("inputMergeTraceTable")
    val outputDateBase: String = conf.get("outputDataBase")
    val outputTable: String = conf.get("outputTable")

    //读取漫出数据
    //    （手机号，开始时间，结束时间，国家编码，国家名称，国家英文名称，来源省份代码，来源省份名称）
    val odValueRDD: RDD[(String, String)] = sparkContext.textFile(conf.get("datPath") + str_date + "_00_001.dat").filter((x: String) => !x.split("\\|", -1)(5).equals("86") && !x.split("\\|", -1)(8).isEmpty).map((line: String) => {

      val calling_no: String = line.split("\\|", -1)(1)
      val start_time: String = line.split("\\|", -1)(3)
      val end_time: String = line.split("\\|", -1)(4)
      val nation_code: String = line.split("\\|", -1)(5)
      val nation_name: String = line.split("\\|", -1)(6)
      val nation_en_name: String = line.split("\\|", -1)(7)
      val come_prov_code: String = line.split("\\|", -1)(14)
      val come_prov_name: String = line.split("\\|", -1)(15)
      (calling_no, calling_no + "\t" + start_time + "\t" + end_time + "\t" + nation_code + "\t" + nation_name + "\t" + nation_en_name + "\t" + come_prov_code + "\t" + come_prov_name)
    })
    if (odValueRDD.count().equals(0)) {
      sparkContext.stop()
    }
    println("odValueRDD" + odValueRDD.count())
    odValueRDD.take(5).foreach {
      println
    }


    // 读取信令数据
    //（归属省份标识，IMSI，电话号码，业务开始时间，ECI，基站经度，基站纬度）
    val readSQL = s"select home_prov_id,imsi,msisdn,busi_start_time,ci,start_longitude,start_latitude from $inputMergeTraceDataBase.$inputMergeTraceTable where stat_hour like concat ('$str_date','%') and msisdn not like concat ('86','%') "
    val readInRDD: RDD[(String, String)] = hiveContext.sql(readSQL).rdd.map((x: Row) => {

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
        msisdn = x.get(2).toString.substring(2)
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
      (msisdn, home_prov_id + "\t" + imsi + "\t" + msisdn + "\t" + busi_start_time + "\t" + eci + "\t" + time + "\t" + start_longitude + "\t" + start_latitude)
    })
    println("readInRDD:" + readInRDD.count())
    readInRDD.take(5).foreach {
      println
    }
    //    odValueRDD.sortBy(x=>x._2,true)
    //
    //    readInRDD.groupByKey().map(x=>{
    //      val xdrContent: List[String] = x._2.toList
    //      (x._1,xdrContent)
    //    })

    //    （手机号，开始时间，结束时间，国家编码，国家名称，国家英文名称，来源省份代码，来源省份名称）
    val whereRDD: RDD[(String, List[Where])] = odValueRDD.map((x: (String, String)) => {
      val where = new Where()
      where.sTime = TimeStampUtils.tranTimeToLong(x._2.split("\t", -1)(1))
      where.eTime = TimeStampUtils.tranTimeToLong(x._2.split("\t", -1)(2))
      where.code = "000000000"
      where.lLA = new LLA(123.123456, 123.123456)
      //      国家编码，国家名称，国家英文名称，来源省份代码
      where.provinceCity = x._2.split("\t", -1)(3) + "\u0001" + x._2.split("\t", -1)(4) + "\u0001" + x._2.split("\t", -1)(5)
      //      （手机号，where）
      (x._2.split("\t", -1)(0), where)
    }).groupByKey()
      .map((x: (String, Iterable[Where])) => {
        val sortWheres: List[Where] = x._2.toList.sortBy((_: Where).sTime)
        (x._1, sortWheres)
      })
    println("whereRDD:" + whereRDD.count())
    for (elem <- whereRDD.take(5)) {
      for (elem <- elem._2) {
        println(elem)
      }
    }


    val map = new mutable.HashMap[String, List[Where]]()
    //    漫出数据较少，转换成广播变量
    val tuples: Array[(String, List[Where])] = whereRDD.collect()
    tuples.map((x: (String, List[Where])) => {
      map.put(x._1, x._2)
    })
    //    漫出数据
    val roam: Broadcast[mutable.HashMap[String, List[Where]]] = sparkContext.broadcast(map)

    //    信令表数据按时间倒序排列，按电话号分组；漫出数据（手机号，Where）。
    //    找出信令中与漫出数据的结束时间最接近的开始时间的那条数据中的eci和imsi。
    val traceRDD: RDD[(String, List[Where])] = readInRDD.filter((x: (String, String)) => {
      val roamValue: mutable.HashMap[String, List[Where]] = roam.value
      roamValue.contains(x._1)
    }).sortBy((x: (String, String)) => x._2.split("\t", -1)(5), ascending = false)
      .groupByKey().map((x: (String, Iterable[String])) => {
      val roamValue: mutable.HashMap[String, List[Where]] = roam.value
      val imsi: String = x._2.head.split("\t", -1)(1) + "\t" + x._2.head.split("\t", -1)(0)
      val wheres: List[Where] = roamValue.getOrElse(x._1, List())
      for (elem <- wheres) {
        if (wheres.indexOf(elem) != wheres.length - 1) {
          if (elem.eTime > wheres(wheres.indexOf(elem) + 1).sTime) {
            elem.eTime = Math.min(elem.eTime, wheres(wheres.indexOf(elem) + 1).sTime)
            wheres(wheres.indexOf(elem) + 1).sTime = Math.min(elem.eTime, wheres(wheres.indexOf(elem) + 1).sTime)
          }
        }
      }
      if (wheres.nonEmpty) {
        for (where <- wheres) {
          //        漫出数据的结束时间
          val eTime: String = TimeStampUtils.longToStringMinute(where.eTime)
          breakable(
            for (xdr <- x._2) {
              if (xdr.nonEmpty) {
                //              信令的开始时间
                val xdrTime: String = TimeStampUtils.longToStringMinute(NumberUtils.toLong(xdr.split("\t", -1)(5)))
                if (xdrTime.equals(eTime)) {
                  where.code = xdr.split("\t", -1)(4)
                  where.lLA = new LLA(NumberUtils.toDouble(xdr.split("\t", -1)(6)), NumberUtils.toDouble(xdr.split("\t", -1)(7)))
                  break()
                }
              }
            }
          )
        }
      }
      (imsi + "|" + x._1, wheres)
    })

    //    （手机号，开始时间，结束时间，下一开始时间，时间间隔，来源省份代码，来源省份名称，到访省份代码，到访省份名称）
    //    （归属省份代码，IMSI，电话号码，业务开始时间，ECI，home）

    println("traceRDD:" + traceRDD.count())
    traceRDD.take(5).foreach {
      println
    }

    val oRDD: RDD[Trace] = traceRDD
      .map((x: (String, List[Where])) => {
        val trace = new Trace(LocType.SECTOR)
        x._2.foreach((y: Where) => {
          trace.loadAsLast(y)
        })
        trace.pohoneNum = x._1.split("\\|", -1)(1) + "\t" + x._1.split("\\|", -1)(0).split("\t", -1)(1)
        trace.imsi = x._1.split("\\|", -1)(0).split("\t", -1)(0)
        trace.date = str_date
        trace
      })
    println("oRDD:" + oRDD.count())


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
    val resultRDD: RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)] = oRDD.flatMap((x: Trace) => {
      val all20list = new ListBuffer[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)]
      var thisCountry = ""
      var lastCountry = ""
      //      国家编码，国家名称，国家英文名称，来源省份代码
      for (elem <- x.wheres) {
        breakable {
          thisCountry = elem.provinceCity.split("\u0001", -1)(0)
          if (thisCountry.isEmpty && lastCountry.isEmpty) {
            thisCountry = elem.provinceCity.split("\u0001", -1)(0)
            lastCountry = elem.provinceCity.split("\u0001", -1)(0)
            break()
          }
          if (!thisCountry.equals(lastCountry) && !thisCountry.isEmpty) {
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
            var ocountryName: String = ""
            if (lastWhere.provinceCity.split("\u0001", -1).isDefinedAt(1)) {
              ocountryName = lastWhere.provinceCity.split("\u0001", -1)(1) + "|" + lastWhere.provinceCity.split("\u0001", -1)(2)
            }
            var ocountryNum: String = ""
            if (lastWhere.provinceCity.split("\u0001", -1).isDefinedAt(0)) {
              ocountryNum = lastWhere.provinceCity.split("\u0001", -1)(0)
            }
            val ocountryScene: String = ""

            //    D点基站归属省编码#
            //    D点基站归属省名称#
            //    D点基站所属场景
            var dcountryName: String = ""
            if (thisWhere.provinceCity.split("\u0001", -1).isDefinedAt(1)) {
              dcountryName = thisWhere.provinceCity.split("\u0001", -1)(1) + "|" + thisWhere.provinceCity.split("\u0001", -1)(2)
            }
            var dcountryNum: String = ""
            if (thisWhere.provinceCity.split("\u0001", -1).isDefinedAt(0)) {
              dcountryNum = thisWhere.provinceCity.split("\u0001", -1)(0)
            }

            val dcountryScene: String = ""

            val phoneNum: String = x.pohoneNum.split("\t", -1)(0)
            val home_province: String = x.pohoneNum.split("\t", -1)(1)

            all20list.append((x.date, x.imsi, phoneNum, lastWhere.code, ocountryNum, ocountryName, ocountryScene, TimeStampUtils.longToTranTime(lastWhere.sTime), TimeStampUtils.longToTranTime(lastWhere.eTime), thisWhere.code, dcountryNum, dcountryName, dcountryScene, TimeStampUtils.longToTranTime(thisWhere.sTime), TimeStampUtils.longToTranTime(thisWhere.eTime), "", home_province))

            lastCountry = thisCountry
          }
        }
      }
      all20list
    })
    println("resultRDD:" + resultRDD.count())

    val rowRDD: RDD[Row] = resultRDD.map((x: (String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)) => Row(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9, x._10, x._11, x._12, x._13, x._14, x._15, x._16, x._17))

    val schema = "date_day,imsi,msisdn,O_cellID,O_country_code,O_country_name,O_cell_state,O_in_time,O_out_time,D_cellID,D_country_code,D_country_name,D_cell_state,D_in_time,D_out_time,OD_country_type,home_province"
    val structType: StructType = StructType(schema.split(",").map((x: String) => StructField(x, StringType, nullable = true)))
    val dataFrame: DataFrame = hiveContext.createDataFrame(rowRDD, structType)
    dataFrame.createOrReplaceTempView("tmpTable")
    val insertSQL: String = s"insert overwrite table $outputDateBase.$outputTable partition(prodate='$str_date',stat_hour='$str_date') " +
      "select date_day,imsi,msisdn,O_cellID,O_country_code,O_country_name,O_cell_state,O_in_time,O_out_time,D_cellID,D_country_code,D_country_name,D_cell_state,D_in_time,D_out_time,OD_country_type,home_province from tmpTable"
    println(insertSQL)
    hiveContext.sql(insertSQL)

    println("success!————国际OD完成" + str_date)

    sparkContext.stop()

  }
}
