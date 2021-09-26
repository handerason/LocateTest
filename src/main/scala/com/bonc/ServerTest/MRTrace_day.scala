package com.bonc.ServerTest

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import com.bonc.data.{LocateType, Record}
import com.bonc.lla.LLA
import com.bonc.location.LocType
import com.bonc.trace.{GTrace, Where}
import com.bonc.user_locate.ReadParam
import com.bonc.xyz.XYZ
import org.apache.commons.lang.math.NumberUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}

/**
 * 用MR数据生成50*50栅格级别的用户轨迹数据
 *
 * @author opslos
 * @date 2021/1/13 10:48:36
 */
object MRTrace_day {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("Warn").setLevel(Level.WARN)
    Logger.getLogger("Error").setLevel(Level.ERROR)

    if (args.length < 2) {
      println("args must be at least 2,xml&procDate")
      System.exit(1)
    }

    val conf_xml: String = args(0)
    val procDate: String = args(1)

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

    val readSQL = s"select grid,calling_imsi,lonLat,start_time,stay_time,end_time,ecis from ${readLib}.${readTable} where prodate=${procDate}"

    val inputRDD: RDD[(String, String, String, String, String, String, String)] = hiveContext.sql(readSQL).rdd.map(x => {
      val grid: String = x.get(0).toString
      val calling_imsi: String = x.get(1).toString
      val lonLat: String = x.get(2).toString
      val start_time: String = x.get(3).toString
      val stay_time: String = x.get(4).toString
      val end_time: String = x.get(5).toString
      val ecis: String = x.get(6).toString
      (calling_imsi, lonLat, grid, start_time, stay_time, end_time, ecis)
    }).filter(_._1.nonEmpty).filter(_._1.nonEmpty).filter(x => {
      // 过滤不是当天的数据
      val day: String = x._4.split(" ")(0).split("-")(2)
      day == procDate.substring(6)
    })

    //    val inputRDD: RDD[(String, String, String, String, String, String, String)] = sparkContext.textFile("C:\\Users\\liubo\\Desktop\\工作\\贵州\\mr轨迹测试\\input\\460000002874120_done.csv")
    //      .map(x => {
    //      val strings: Array[String] = x.split("\\*")
    //      val grid: String = strings(0).toString
    //      val calling_imsi: String = strings(1).toString
    //      val lonLat: String = strings(2).toString
    //      val start_time: String = strings(3).toString
    //      val stay_time: String = strings(4).toString
    //      val end_time: String = strings(5).toString
    //      val ecis: String = strings(6).toString
    //      (calling_imsi, lonLat, grid, start_time, stay_time, end_time, ecis)
    //    }).filter(_._1.nonEmpty).filter(x => {
    //      // 过滤不是当天的数据
    //      val day: String = x._4.split(" ")(0).split("-")(2)
    //      day == procDate.substring(6)
    //    })

    val makeRecordRDD: RDD[(String, Where)] = inputRDD.map(x => {
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
      val where = new Where()
      where.lLA = new LLA(NumberUtils.toDouble(x._2.split(",")(0)), NumberUtils.toDouble(x._2.split(",")(1)))
      where.sTime = dateFormat.parse(x._4).getTime
      where.eTime = dateFormat.parse(x._6).getTime
      where.duration = NumberUtils.toInt(x._5)
      where.code = x._3
      for (elem <- x._7.split("#").toList) {
        where.ecis.append(NumberUtils.toLong(elem))
      }
      val str: String = x._1
      (str, where)
    })

    val makeTraceRDD: RDD[GTrace] = makeRecordRDD.groupBy(_._1).map(x => {
      val imsi: String = x._1
      val wheres = new ListBuffer[Where]
      x._2.foreach(y => {
        wheres.append(y._2)
      })

      val sortWheres: ListBuffer[Where] = wheres.sortBy(_.sTime)
      val addWheres: ListBuffer[Where] = new ListBuffer[Where]

      val trace = new GTrace(LocType.GRID)
      for (elem <- sortWheres) {
        breakable {
          if (sortWheres.indexOf(elem) == 0) {
            break()
          } else {
            val lastWhere: Where = sortWheres(sortWheres.indexOf(elem) - 1)
            val thisWhere: Where = elem
            // 栅格ID相同
            //            if (lastWhere.code.equals(thisWhere.code)) {
            if (lastWhere.eTime <= thisWhere.sTime) {
              addWheres.append(lastWhere)
              if (sortWheres.indexOf(thisWhere) == sortWheres.length - 1) {
                addWheres.append(thisWhere)
              }
              break()
            } else {
              thisWhere.sTime = math.min(thisWhere.sTime, lastWhere.eTime)
              lastWhere.eTime = math.min(thisWhere.sTime, lastWhere.eTime)
              if (sortWheres.indexOf(thisWhere) == sortWheres.length - 1) {
                addWheres.append(lastWhere)
                addWheres.append(thisWhere)
              } else {
                addWheres.append(lastWhere)
              }
            }
          }
        }
      }
      val sortAddWhere: ListBuffer[Where] = addWheres.distinct.sortBy(_.sTime)

      trace.imsi = imsi
      trace.createByWhere(sortAddWhere.toList)
      trace.date = procDate
      trace
    })

    val resultRDD: RDD[(String, String, String, String, String, String, String, String, String, String)] = makeTraceRDD.flatMap(x => {
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
      // 数据日期,手机号,栅格ID,IMEI,IMSI,开始时间,停留时间,结束时间,栅格内ECI列表,轨迹数据源类型
      val tuples = new ListBuffer[(String, String, String, String, String, String, String, String, String, String)]
      x.wheres.foreach(y => {
        val sDate: String = dateFormat.format(new Date(y.sTime))
        val eDate: String = dateFormat.format(new Date(y.eTime))
        //        val listBuffer: ListBuffer[Long] = x.ecis(y.code)
        val stringBuilder = new StringBuilder
        for (elem <- y.ecis) {
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
    val insertSQL: String = s"insert overwrite table ${insertLib}.${insertTable} partition(prodate='" + procDate + "') " +
      "select calling_no,grid,calling_imei,calling_imsi,lonLat,start_time,stay_time,end_time,ecis,source from tmpTable"
    println(insertSQL)
    hiveContext.sql(insertSQL)
    //    rowRDD.repartition(1).saveAsTextFile("C:\\Users\\liubo\\Desktop\\工作\\贵州\\mr轨迹测试\\output\\result4")
    println("InOutFlowAnalyze-------SUCCESS!")
    sparkContext.stop()


  }
}


































