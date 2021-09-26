package com.bonc.ServerTestRebuild

import java.io.BufferedReader
import java.text.SimpleDateFormat
import java.util.Date

import com.bonc.data.LocateType
import com.bonc.lla.LLA
import com.bonc.location.LocType
import com.bonc.trace.{Trace, Where}
import com.bonc.user_locate.ReadParam
import com.bonc.user_locate.utils.FileUtil
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
 * 区县流入流出模型
 *
 * @author opslos
 * @date 2021/4/27 14:12:30
 */
object districtInOutFlow {
  def main(args: Array[String]): Unit = {

    //日志级别设置
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

    val conf: SparkConf = new SparkConf().setAppName("districtInOutFlow")
    ReadParam.readXML(conf, configPath)

    val isLocal: String = conf.get("local")
    if ("1".equals(isLocal)) {
      conf.setMaster("local[*]")
    }

    val inputDateBase: String = conf.get("inputDataBase")
    val inputTale: String = conf.get("inputTale")
    val outputDateBase: String = conf.get("outputDataBase")
    val outputTable: String = conf.get("outputTable")

    val sc = new SparkContext(conf) // 创建SparkContext
    val hiveContext = new HiveContext(sc)

    // 读取基站进出模型
    val readSQL = s"select imsi,city_code,county_code,enter_time,leave_time,longitude,latitude from ${inputDateBase}.${inputTale} where day_id=${str_date}"
    val inputRDD: RDD[(String, String, String, String, String, String, String)] = hiveContext.sql(readSQL).rdd.map((x: Row) => {
      var imsi: String = ""
      if (!x.isNullAt(0) && !x.get(0).toString.isEmpty) {
        imsi = x.get(0).toString
      }
      var city_code: String = ""
      if (!x.isNullAt(1) && !x.get(1).toString.isEmpty) {
        city_code = x.get(1).toString
      }
      var county_code: String = ""
      if (!x.isNullAt(2) && !x.get(2).toString.isEmpty) {
        county_code = x.get(2).toString
      }
      var enter_time: String = ""
      if (!x.isNullAt(3) && !x.get(3).toString.isEmpty) {
        enter_time = x.get(3).toString
      }
      var leave_time: String = ""
      if (!x.isNullAt(4) && !x.get(4).toString.isEmpty) {
        leave_time = x.get(4).toString
      }
      var longitude: String = ""
      if (!x.isNullAt(5) && !x.get(5).toString.isEmpty) {
        longitude = x.get(5).toString
      }
      var latitude: String = ""
      if (!x.isNullAt(6) && !x.get(6).toString.isEmpty) {
        latitude = x.get(6).toString
      }
      (imsi, city_code, county_code, enter_time, leave_time, longitude, latitude)
    })
    println("inputRDD：" + inputRDD.count())

    val wheresRDD: RDD[(String, Where)] = inputRDD.map((x: (String, String, String, String, String, String, String)) => {
      val where = new Where()
      val district: String = x._2 + "_" + x._3
      where.code = district
      where.sTime = NumberUtils.toLong(x._4)
      where.eTime = NumberUtils.toLong(x._5)
      where.lLA = new LLA(NumberUtils.toDouble(x._6), NumberUtils.toDouble(x._7))
      (x._1, where)
    })
    println("wheresRDD：" + wheresRDD.count())

    val traceRDD: RDD[Trace] = wheresRDD.groupByKey().map((x: (String, Iterable[Where])) => {
      val sortWheres: List[Where] = x._2.toList.sortBy((_: Where).sTime)
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
        x._2.foreach((y: Where) => {
          trace.loadAsLast(y)
        })
        trace.imsi = x._1
        trace.date = str_date
        trace
      })
    println("traceRDD：" + traceRDD.count())

    val districtFlow: RDD[(String, String, String, String, String, String, Long, String)] = traceRDD.flatMap((x: Trace) => {
      val resultList = new ListBuffer[(String, String, String, String, String, String, Long, String)]
      // 第一个始终为流出，第二个始终为流入
      // 出发区县,到达区县,来源或去向,时间,imsi
      val wheres: List[Where] = x.wheres.toList.sortBy((_: Where).sTime)
      for (elem <- wheres) {
        breakable {
          if (wheres.indexOf(elem) == 0 || wheres.length == 1) {
            break()
          } else {
            val lastWhere: Where = wheres(wheres.indexOf(elem) - 1)
            val thisWhere: Where = elem
            // 出发区县以及时间
            var outFlowDistrict: String = ""
            if (lastWhere.code.split("_", -1).isDefinedAt(1)) {
              outFlowDistrict = lastWhere.code.split("_", -1)(1)
            }
            var outFlowCity: String = ""
            if (lastWhere.code.split("_", -1).isDefinedAt(0)) {
              outFlowCity = lastWhere.code.split("_", -1)(0)
            }
            var inFlowDistrict: String = ""
            if (thisWhere.code.split("_", -1).isDefinedAt(1)) {
              inFlowDistrict = thisWhere.code.split("_", -1)(1)
            }
            var inFlowCity: String = ""
            if (thisWhere.code.split("_", -1).isDefinedAt(0)) {
              inFlowCity = thisWhere.code.split("_", -1)(0)
            }
            val outFlowTime: Long = lastWhere.eTime
            val inFlowTime: Long = thisWhere.sTime
            resultList.append((x.date, outFlowCity, outFlowDistrict, inFlowCity, inFlowDistrict, "out", outFlowTime, x.imsi))

            resultList.append((x.date, inFlowCity, inFlowDistrict, outFlowCity, outFlowDistrict, "in", inFlowTime, x.imsi))
          }
        }
      }
      resultList
    })
    println("districtFlow：" + districtFlow.count())

    val format = new SimpleDateFormat("HH")

    val countRDD: RDD[(String, String, String, String, String, String, String, String)] = districtFlow.map((x: (String, String, String, String, String, String, Long, String)) => {
      val hh: String = format.format(new Date(x._7))
      val nextHH: String = f"${NumberUtils.toInt(hh) + 1}%02d"
      val priod: String = hh + "-" + nextHH
      val keyW: String = x._2 + "\u0001" + x._3 + "\u0001" + x._4 + "\u0001" + x._5 + "\u0001" + x._6 + "\u0001" + priod
      (keyW, x._8)
    }).groupByKey()
      .map((x: (String, Iterable[String])) => {
        val strings: Array[String] = x._1.split("\u0001", -1)
        (str_date, strings(0), strings(1), strings(2), strings(3), strings(4), strings(5), x._2.toList.distinct.size.toString)
      })
    println("countRDD：" + countRDD.count())

    val rowRDD: RDD[Row] = countRDD.map((x: (String, String, String, String, String, String, String, String)) => {
      Row(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8)
    })
    val schema = "date_day,city1,district1,city2,district2,direction,time_second,user_num"
    val structType: StructType = StructType(schema.split(",").map((x: String) => StructField(x, StringType, nullable = true)))
    val dataFrame: DataFrame = hiveContext.createDataFrame(rowRDD, structType)
    dataFrame.createOrReplaceTempView("tmpTable")
    val insertSQL: String = s"insert overwrite table ${outputDateBase}.${outputTable} partition(prodate='" + str_date + "') " +
      "select date_day,city1,district1,city2,district2,direction,time_second,user_num from tmpTable"
    println(insertSQL)
    hiveContext.sql(insertSQL)

    sc.stop()
    println("success!————区县迁入迁出模型" + str_date)

  }
}

























