package com.bonc.ServerTestRebuild

import com.bonc.lla.LLA
import com.bonc.location.LocType
import com.bonc.trace.{Trace, Where}
import com.bonc.user_locate.ReadParam
import com.bonc.user_locate.utils.getGeoHash
import org.apache.commons.lang.math.NumberUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable.ListBuffer

/**
 * 用户话单和融合定位结果进行生成用户的融合轨迹
 *
 * @author opslos
 * @date 2021/4/29 17:02:15
 */
object billComineMerge {
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

    val str_hour = args.length match {
      case 1 => null
      case 2 => null
      case 3 => args(2)
      case _ => null
    }


    val conf = new SparkConf().setAppName("billComineMerge")
    ReadParam.readXML(conf, configPath)

    val isLocal = conf.get("local")
    if ("1".equals(isLocal)) {
      conf.setMaster("local[*]")
    }

    val inputBillDataBase: String = conf.get("inputBillDataBase")
    val inputBillTale: String = conf.get("inputBillTale")
    val inputLocalDataBase: String = conf.get("inputLocalDataBase")
    val inputLocalTable: String = conf.get("inputLocalTable")
    val outputDataBase: String = conf.get("outputDataBase")
    val outputTable: String = conf.get("outputTable")

    val sc = new SparkContext(conf) // 创建SparkContext
    val hiveContext = new HiveContext(sc)

    // 读取话单轨迹数据
    val readBillSQL = s"select imsi,msisdn,imei,lac_cell,rat,longitude,latitude,geohash,country_code,city_code,county_code,enter_time,leave_time,source from ${inputBillDataBase}.${inputBillTale} where prodate='${str_date}' and hour_id=${str_hour}"
    val billTraceReadRDD: RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String, String)] = hiveContext.sql(readBillSQL).rdd.map(x => {
      var imsi: String = ""
      if (!x.isNullAt(0) && !x.get(0).toString.isEmpty) {
        imsi = x.get(0).toString
      }
      var msisdn: String = ""
      if (!x.isNullAt(1) && !x.get(1).toString.isEmpty) {
        msisdn = x.get(1).toString
      }
      var imei: String = ""
      if (!x.isNullAt(2) && !x.get(2).toString.isEmpty) {
        imei = x.get(2).toString
      }
      var lac_cell: String = ""
      if (!x.isNullAt(3) && !x.get(3).toString.isEmpty) {
        lac_cell = x.get(3).toString
      }
      var rat: String = ""
      if (!x.isNullAt(4) && !x.get(4).toString.isEmpty) {
        rat = x.get(4).toString
      }
      var longitude: String = ""
      if (!x.isNullAt(5) && !x.get(5).toString.isEmpty) {
        longitude = x.get(5).toString
      }
      var latitude: String = ""
      if (!x.isNullAt(6) && !x.get(6).toString.isEmpty) {
        latitude = x.get(6).toString
      }
      var geohash: String = ""
      if (!x.isNullAt(7) && !x.get(7).toString.isEmpty) {
        geohash = x.get(7).toString
      }
      var country_code: String = ""
      if (!x.isNullAt(8) && !x.get(8).toString.isEmpty) {
        country_code = x.get(8).toString
      }
      var city_code: String = ""
      if (!x.isNullAt(9) && !x.get(9).toString.isEmpty) {
        city_code = x.get(9).toString
      }
      var county_code: String = ""
      if (!x.isNullAt(10) && !x.get(10).toString.isEmpty) {
        county_code = x.get(10).toString
      }
      var enter_time: String = ""
      if (!x.isNullAt(11) && !x.get(11).toString.isEmpty) {
        enter_time = x.get(11).toString
      }
      var leave_time: String = ""
      if (!x.isNullAt(12) && !x.get(12).toString.isEmpty) {
        leave_time = x.get(12).toString
      }
      var source: String = ""
      if (!x.isNullAt(13) && !x.get(13).toString.isEmpty) {
        source = x.get(13).toString
      }
      (imsi, msisdn, imei, lac_cell, rat, longitude, latitude, geohash, country_code, city_code, county_code, enter_time, leave_time, source)
    })
    println("billTraceReadRDD" + billTraceReadRDD.count())


    // 读取融合定位轨迹数据
    val readCombineSQL = s"select imsi,msisdn,imei,lac_cell,rat,longitude,latitude,geohash,country_code,city_code,county_code,enter_time,leave_time,source from ${inputLocalDataBase}.${inputLocalTable} where day_id='${str_date}'"
    val combineTraceReadRDD: RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String, String)] = hiveContext.sql(readCombineSQL).rdd.map(x => {
      var imsi: String = ""
      if (!x.isNullAt(0) && !x.get(0).toString.isEmpty) {
        imsi = x.get(0).toString
      }
      var msisdn: String = ""
      if (!x.isNullAt(1) && !x.get(1).toString.isEmpty) {
        msisdn = x.get(1).toString
      }
      var imei: String = ""
      if (!x.isNullAt(2) && !x.get(2).toString.isEmpty) {
        imei = x.get(2).toString
      }
      var lac_cell: String = ""
      if (!x.isNullAt(3) && !x.get(3).toString.isEmpty) {
        lac_cell = x.get(3).toString
      }
      var rat: String = ""
      if (!x.isNullAt(4) && !x.get(4).toString.isEmpty) {
        rat = x.get(4).toString
      }
      var longitude: String = ""
      if (!x.isNullAt(5) && !x.get(5).toString.isEmpty) {
        longitude = x.get(5).toString
      }
      var latitude: String = ""
      if (!x.isNullAt(6) && !x.get(6).toString.isEmpty) {
        latitude = x.get(6).toString
      }
      var geohash: String = ""
      if (!x.isNullAt(7) && !x.get(7).toString.isEmpty) {
        geohash = x.get(7).toString
      }
      var country_code: String = ""
      if (!x.isNullAt(8) && !x.get(8).toString.isEmpty) {
        country_code = x.get(8).toString
      }
      var city_code: String = ""
      if (!x.isNullAt(9) && !x.get(9).toString.isEmpty) {
        city_code = x.get(9).toString
      }
      var county_code: String = ""
      if (!x.isNullAt(10) && !x.get(10).toString.isEmpty) {
        county_code = x.get(10).toString
      }
      var enter_time: String = ""
      if (!x.isNullAt(11) && !x.get(11).toString.isEmpty) {
        enter_time = x.get(11).toString
      }
      var leave_time: String = ""
      if (!x.isNullAt(12) && !x.get(12).toString.isEmpty) {
        leave_time = x.get(12).toString
      }
      var source: String = ""
      if (!x.isNullAt(13) && !x.get(13).toString.isEmpty) {
        source = x.get(13).toString
      }
      (imsi, msisdn, imei, lac_cell, rat, longitude, latitude, geohash, country_code, city_code, county_code, enter_time, leave_time, source)
    })
    println("combineTraceReadRDD" + combineTraceReadRDD.count())


    val mergeTraceRDD: RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String, String)] = combineTraceReadRDD.union(billTraceReadRDD)


    val resultRDD: RDD[Trace] = mergeTraceRDD.map(x => {
      val imsi: String = x._1
      val where = new Where()

      if (x._4.nonEmpty) {
        where.code = x._4
      }
      if (x._6.nonEmpty && x._7.nonEmpty) {
        where.lLA = new LLA(NumberUtils.toDouble(x._6), NumberUtils.toDouble(x._7))
      } else {
        where.lLA = new LLA(0D, 0D)
      }
      if (x._9.nonEmpty || x._10.nonEmpty || x._11.nonEmpty) {
        where.provinceCity = x._9 + "_" + x._10 + "_" + x._11
      }
      if (x._12.nonEmpty) {
        where.sTime = NumberUtils.toLong(x._12)
      }
      if (x._13.nonEmpty) {
        where.eTime = NumberUtils.toLong(x._13)
      }
      where.source = x._14

      (imsi+"\u0001"+x._2,where)
    }).groupByKey()
      .map(x=>{
        val sortWheres: List[Where] = x._2.toList.sortBy(_.sTime)
        for (elem <- sortWheres) {
          if (sortWheres.indexOf(elem) != sortWheres.length -1){
            if (elem.eTime > sortWheres(sortWheres.indexOf(elem)+1).sTime){
              elem.eTime = Math.min(elem.eTime,sortWheres(sortWheres.indexOf(elem)+1).sTime)
              sortWheres(sortWheres.indexOf(elem)+1).sTime = Math.min(elem.eTime,sortWheres(sortWheres.indexOf(elem)+1).sTime)
            }
          }
        }
        (x._1,sortWheres)
      })
      .map(x => {
      val trace = new Trace(LocType.SECTOR)
      trace.imsi = x._1.split("\u0001",-1)(0)
      x._2.foreach(y => {
        trace.loadAsLast(y)
      })
      trace.pohoneNum = if (x._1.split("\u0001",-1)(1).isDefinedAt(1)) x._1.split("\u0001",-1)(1) else ""
      trace.date = str_date
      trace
    })
    println("mergeTraceRDD" + mergeTraceRDD.count())

    val billTraceRDD: RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String, String)] = resultRDD.flatMap(x => {
      val listBuffer = new ListBuffer[(String, String, String, String, String, String, String, String, String, String, String, String, String, String)]
      x.wheres.foreach(y => {
        var country_code: String = ""
        if (y.provinceCity.split("_", -1).isDefinedAt(0)) {
          country_code = y.provinceCity.split("_",-1)(0)
        }
        var city_code: String = ""
        if (y.provinceCity.split("_", -1).isDefinedAt(1)) {
          city_code = y.provinceCity.split("_",-1)(1)
        }
        var county_code: String = ""
        if (y.provinceCity.split("_", -1).isDefinedAt(2)) {
          county_code = y.provinceCity.split("_",-1)(2)
        }
        var geocode = ""
        if (y.lLA.longitude != 0D && y.lLA.latitude != 0D) {
          geocode = getGeoHash.encode(y.lLA.longitude, y.lLA.latitude)
        }
        listBuffer.append((x.imsi, x.pohoneNum, "", y.code.toString, "", y.lLA.longitude.toString, y.lLA.latitude.toString, geocode, country_code, city_code, county_code, y.sTime.toString, y.eTime.toString, y.source))
      })
      listBuffer
    })
    println("billTraceRDD" + billTraceRDD.count())


    val resultRow: RDD[Row] = billTraceRDD.map(x => Row(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9, x._10, x._11, x._12, x._13, x._14))
    val schema = "imsi,msisdn,imei,lac_cell,rat,longitude,latitude,geohash,country_code,city_code,county_code,enter_time,leave_time,source"
    val structType: StructType = StructType(schema.split(",").map(x => StructField(x, StringType, true)))
    val dataFrame: DataFrame = hiveContext.createDataFrame(resultRow, structType)
    dataFrame.createOrReplaceTempView("tmpTable")
    val insertSQL: String = s"insert overwrite table ${outputDataBase}.${outputTable} partition(day_id='${str_date}',hour_id='${str_hour}') " +
      "select imsi,msisdn,imei,lac_cell,rat,longitude,latitude,geohash,country_code,city_code,county_code,enter_time,leave_time,source from tmpTable"
    println(insertSQL)
    hiveContext.sql(insertSQL)

    sc.stop()
    println("success!————话单融合定位数据融合" + str_date)


  }

}































