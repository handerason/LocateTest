package com.bonc.ServerTestRebuild

import java.io.BufferedReader
import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date, TimeZone}

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

/**
 * @Author: whzHander 
 * @Date: 2021/8/10 17:18 
 * @Description:
 * This code sucks, you know it and I know it.
 * Move on and call me an idiot later.
 * If this code works, it was written by Wang Hengze.
 * If not, I don't know who wrote it.
 */
object administrativeRegionFlowTest {
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

    val conf: SparkConf = new SparkConf().setAppName("administrativeRegionFlow")
    ReadParam.readXML(conf, configPath)

    val isLocal: String = conf.get("local")
    if ("1".equals(isLocal)) {
      conf.setMaster("local[*]")
    }

    val inputDataBase: String = conf.get("inputDataBase")
    val inputTale: String = conf.get("inputTale")
    //    val inputEciAdminBase: String = conf.get("inputEciAdminBase")
    //    val inputEciAdminTale: String = conf.get("inputEciAdminTale")
    val outputDataBase: String = conf.get("outputDataBase")
    val outputTable: String = conf.get("outputTable")

    val sc = new SparkContext(conf) // 创建SparkContext
    val hiveContext = new HiveContext(sc)



    // 加载行政区码表
    //        val adminSQL = s"select eci,admin from ${inputEciAdminBase}.${inputEciAdminTale}"
    //        val eciAdminRDD: RDD[(String, String)] = hiveContext.sql(adminSQL).rdd.map(x => {
    //          val eci: String = x.get(0).toString
    //          val adminID: String = x.get(1).toString
    //          val adminName: String = x.get(2).toString
    //          val adminLLAs: String = x.get(3).toString
    //          val str: String = adminID + "\t" + adminName + "\t" + adminLLAs
    //          (eci, str)
    //        })
    //        val eciAdminBroad: Broadcast[collection.Map[String, String]] = sc.broadcast(eciAdminRDD.collectAsMap())

    //读取mergeTraceCellFlow
    val inSQL = s"select imsi,lac_cell,rat,longitude,latitude,enter_time,leave_time,county_code from ${inputDataBase}.${inputTale} where day_id=${str_date}"
    val inputRDD: RDD[(String, String, String, String, String, String, String, String)] = hiveContext.sql(inSQL).rdd.map((x: Row) => {
      var imsi: String = ""
      if (!x.isNullAt(0) && !x.get(0).toString.isEmpty) {
        imsi = x.get(0).toString
      }
      var lac_cell: String = ""
      if (!x.isNullAt(1) && !x.get(1).toString.isEmpty) {
        lac_cell = x.get(1).toString
      }
      var rat: String = ""
      if (!x.isNullAt(2) && !x.get(2).toString.isEmpty) {
        rat = x.get(2).toString
      }
      var longitude: String = ""
      if (!x.isNullAt(3) && !x.get(3).toString.isEmpty) {
        longitude = x.get(3).toString
      }
      var latitude: String = ""
      if (!x.isNullAt(4) && !x.get(4).toString.isEmpty) {
        latitude = x.get(4).toString
      }
      var enter_time: String = ""
      if (!x.isNullAt(5) && !x.get(5).toString.isEmpty) {
        enter_time = x.get(5).toString
      }
      var leave_time: String = ""
      if (!x.isNullAt(6) && !x.get(6).toString.isEmpty) {
        leave_time = x.get(6).toString
      }
      var county_code: String = ""
      if (!x.isNullAt(7) && !x.get(7).toString.isEmpty) {
        county_code = x.get(7).toString
      }
      (imsi, lac_cell, rat, longitude, latitude, enter_time, leave_time, county_code)
    })
    println("inputRDD" + inputRDD.count())


    //读取hdfs：YDY_XZQ_ECI_DATA.txt
    val dataTextFile: BufferedReader = FileUtil.getBufferedReader(new Path(conf.get("G4Path")))
    var line: String = ""
    line = dataTextFile.readLine()
    val list = new mutable.HashMap[String, String]()
    while (line != null) {
      if (!line.contains("-")) {
        if (line.split("\\|").length >= 5) {
          //          eci，场景id 场景名称 地市id 地市名称 区县id 区县名称
          list.put(line.split("\\|")(2), line.split("\\|")(0) + "\t" + line.split("\\|")(1) + "\t" + line.split("\\|")(3) + "\t" + line.split("\\|")(4) + "\t" + line.split("\\|")(5) + "\t" + line.split("\\|")(6))
        }
      }
      line = dataTextFile.readLine()
    }
    dataTextFile.close()
    println("list" + list.size)
    val eciAdminBroad: Broadcast[mutable.HashMap[String, String]] = sc.broadcast(list)


    //读取hdfs：YDY_ECI_DATA.txt
    //    val dataTextFileRDD: RDD[(String)] = sc.textFile(args(3))
    //    val inputRDD: RDD[(String, String, String, String, String, String, String)] = dataTextFileRDD.flatMap(_.split("|")).map(x => {
    //      val sceneId: String = x.get(0)
    //      val sceneName: String = x.get(1)
    //      val eci: String = x.get(2)
    //      val cityId: String = x.get(3)
    //      val cityName: String = x.get(4)
    //      val countryId: String = x.get(5)
    //      val countryName: String = x.get(6)
    //      (sceneId, sceneName, eci, cityId, cityName, countryId, countryName)
    //    })

    //  imsi,lac_cell,rat,longitude,latitude,enter_time,leave_time,county_code
    val traceRDD: RDD[Trace] = inputRDD.map((x: (String, String, String, String, String, String, String, String)) => {
      val where = new Where()
      if (!x._6.equals("0")) {
        where.sTime = NumberUtils.toLong(x._6)
      }
      if (!x._6.equals("0")) {
        where.eTime = NumberUtils.toLong(x._7)
      }
      where.code = x._2
      where.lLA = new LLA(NumberUtils.toDouble(x._4), NumberUtils.toDouble(x._5))
      where.rat = NumberUtils.toInt(x._3)
      (x._1, where)
    }).groupByKey()
      .map((x: (String, Iterable[Where])) => {
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
    println("traceRDD" + traceRDD.count())

    //          eci，场景id 场景名称 地市id 地市名称 区县id 区县名称
    val adminTraceRDD: RDD[Trace] = traceRDD.map((x: Trace) => {
      val ecis: mutable.HashMap[String, String] = eciAdminBroad.value
      x.wheres.foreach((y: Where) => {
        val countyId: String = ecis.getOrElse(y.code, "")
        if (countyId.nonEmpty && countyId.split("\t", -1).isDefinedAt(4)) {
          y.provinceCity = ecis.getOrElse(y.code, "").split("\t", -1)(4) + "\t" + ecis.getOrElse(y.code, "").split("\t", -1)(5)
        }
      })
      // 进行where的合并
      val whereList: List[Where] = x.wheres.toList.sortBy((_: Where).sTime)
      val trace = new Trace(x.imsi, x.traceType)
      whereList.foreach((y: Where) => {
        trace.loadAsLast(y)
      })
      trace
    })
    println("adminTraceRDD" + adminTraceRDD.count())


    val eciTraceRDD: RDD[(String, String, String, String, String, String)] = adminTraceRDD.flatMap((x: Trace) => {
      // 时间段，区县id，保持人的imsi，流入人的imsi，流出人的imsi，区域坐标串
      val resultList = new ListBuffer[(String, String, String, String, String, String)]

      val hourFormat = new SimpleDateFormat("yyyyMMdd HH:00:00")
      hourFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))

      x.wheres.toList.sortBy((_: Where).sTime).foreach((y: Where) => {
        val sHourStamp: String = hourFormat.format(new Date(y.sTime))
        val eHourStamp: String = hourFormat.format(new Date(y.eTime))
        val sHour: Int = NumberUtils.toInt(sHourStamp.substring(9, 11))
        val eHour: Int = NumberUtils.toInt(eHourStamp.substring(9, 11))
        if (sHourStamp.equals(eHourStamp)) {
          resultList.append((sHourStamp, y.provinceCity, "", x.imsi, "", y.lLA.toString))
          resultList.append((eHourStamp, y.provinceCity, "", "", x.imsi, y.lLA.toString))
        } else if (sHour < eHour) {
          val diff: Int = eHour - sHour
          // 驻留时间
          for (i <- Range(1, diff)) {
            val calendar: Calendar = Calendar.getInstance()
            calendar.setTime(new Date(y.sTime))
            calendar.add(Calendar.HOUR, i)
            val upTime: Date = calendar.getTime
            val upHour: String = hourFormat.format(upTime)
            resultList.append((upHour, y.provinceCity, x.imsi, "", "", y.lLA.toString))
          }
          // 进入
          resultList.append((sHourStamp, y.provinceCity, "", x.imsi, "", y.lLA.toString))
          // 离开
          resultList.append((eHourStamp, y.provinceCity, "", "", x.imsi, y.lLA.toString))
        }
      })
      resultList
    })
    println("eciTraceRDD" + eciTraceRDD.count())
    for (elem <- eciTraceRDD.take(5)) {
      println(elem)
    }

    // 时间段，区县id，保持人的imsi，流入人的imsi，流出人的imsi
    val resultRDD: RDD[(String, String, String, String, String, String, String, String, String)] = eciTraceRDD.map((x: (String, String, String, String, String, String)) => {
      val str: String = x._1 + "\\|" + x._2
      (str, x._3, x._4, x._5, x._6)
    }).groupBy((_: (String, String, String, String, String))._1).flatMap((x: (String, Iterable[(String, String, String, String, String)])) => {
      val listBuffer = new ListBuffer[(String, String, String, String, String, String, String, String, String)]
      val hourStamp: String = x._1.split("\\|", -1)(0)
      val area_id: String = x._1.split("\\|", -1)(1).split("\t", -1)(0)
      var area_name: String = ""
      if (x._1.split("\\|", -1)(1).split("\t", -1).isDefinedAt(1)) {
        area_name = x._1.split("\\|", -1)(1).split("\t", -1)(1)
      }
      val stay = new ListBuffer[String]
      val inFlow = new ListBuffer[String]
      val outFlow = new ListBuffer[String]
      val LLAs = new ListBuffer[String]
      x._2.foreach((x: (String, String, String, String, String)) => {
        stay.append(x._2)
        inFlow.append(x._3)
        outFlow.append(x._4)
        LLAs.append(x._5)
      })
      val stayCount: Int = stay.count((y: String) => y.nonEmpty)
      val inFlowCount: Int = inFlow.count((y: String) => y.nonEmpty)
      val outFlowCount: Int = outFlow.count((y: String) => y.nonEmpty)
      val totalNum: Int = stayCount + inFlowCount - outFlowCount
      listBuffer append ((hourStamp, area_id, area_name, LLAs.toString(), totalNum.toString, stayCount.toString, inFlowCount.toString, outFlowCount.toString, hourStamp.split(" ")(0)))
      listBuffer
    })
    println("resultRDD:" + resultRDD.count())

    val rowRDD: RDD[Row] = resultRDD.map((x: (String, String, String, String, String, String, String, String, String)) => {
      Row(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8)
    })
    val schema = "hourStamp,code,name,llAs,totalNum,stayCount,inFlowCount,outFlowCount"
    val structType: StructType = StructType(schema.split(",").map((x: String) => StructField(x, StringType, nullable = true)))
    val dataFrame: DataFrame = hiveContext.createDataFrame(rowRDD, structType)
    dataFrame.createOrReplaceTempView("tmpTable")
    val insertSQL: String = s"insert overwrite table ${outputDataBase}.${outputTable} partition(day_id='" + str_date + "') " +
      "select hourStamp,code,name,llAs,totalNum,stayCount,inFlowCount,outFlowCount from tmpTable"
    println(insertSQL)
    hiveContext.sql(insertSQL)

    sc.stop()
    println("success!————行政区&&自定义区域迁入迁出模型" + str_date)
  }
}
