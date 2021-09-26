package com.bonc.ServerTestRebuild

import java.io.BufferedReader

import com.bonc.data.{LocateType, Record}
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
 * @Author: whzHander
 * @Date: 2021/8/18 16:21
 * @Description:
 * This code sucks, you know it and I know it.
 * Move on and call me an idiot later.
 * If this code works, it was written by Wang Hengze.
 * If not, I don't know who wrote it.
 */
object countyBetweenODTest {
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

    val conf: SparkConf = new SparkConf().setAppName("countyBetweenOD")
    ReadParam.readXML(conf, configPath)

    val isLocal: String = conf.get("local")
    if ("1".equals(isLocal)) {
      conf.setMaster("local[*]")
    }

    val inputDateBase: String = conf.get("inputDataBase")
    val inputTable: String = conf.get("inputTable")
    val outputDateBase: String = conf.get("outputDataBase")
    val outputTable: String = conf.get("outputTable")

    val sc = new SparkContext(conf) // 创建SparkContext
    val hiveContext = new HiveContext(sc)


    val gcReader: BufferedReader = FileUtil.getBufferedReader(new Path(conf.get("G4Path")))
    val G2List = new mutable.HashMap[String, String]()
    val G4List = new mutable.HashMap[String, String]()
    var gc: String = ""
    gc = gcReader.readLine()
    println(gc)
    while (gc != null) {
      if (!gc.contains("-")) {
        // 城市+“_”+区县+“_”+场景
        if (gc.split("\\|")(2).contains("_")) {
          G2List.put(gc.split("\\|")(2), gc.split("\\|")(5) + "_" + gc.split("\\|")(5) + "_" + gc.split("\\|")(0))
        } else {
          G4List.put(gc.split("\\|")(2), gc.split("\\|")(5) + "_" + gc.split("\\|")(5) + "_" + gc.split("\\|")(0))
        }
        gc = gcReader.readLine()
      }
      gcReader.close()
      println("G4List" + G4List.size)
      println("G2List" + G2List.size)
      val G4Broad: Broadcast[mutable.HashMap[String, String]] = sc.broadcast(G4List)
      val G2Broad: Broadcast[mutable.HashMap[String, String]] = sc.broadcast(G2List)


      // 读取融合定位数据
      val readSQL = s"select imsi,loctype,stime,maincellid,longitude,latitude from $inputDateBase.$inputTable where day_id='$str_date'"
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
      println(inputRDD.count())

      val traceRDD: RDD[Trace] = inputRDD.map((x: (String, String, String, String, String, String)) => {
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
      }).groupByKey()
        .map((x: (String, Iterable[Record])) => {
          val trace = new Trace(LocType.SECTOR)
          val records: List[Record] = x._2.toList.sortBy((_: Record).sTime)
          trace.create(x._1, records)
          trace
        })
      println("traceRDD:" + traceRDD.count())
      traceRDD.take(5).foreach {
        println
      }

      val resultRDD: RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)] = traceRDD.map((x: Trace) => {
        //      eci，地市_区县_场景
        val g2Broad: mutable.HashMap[String, String] = G2Broad.value
        val g4Broad: mutable.HashMap[String, String] = G4Broad.value
        x.wheres.foreach((y: Where) => {
          var cellInfo: String = ""
          if (y.rat == 2 && g2Broad.contains(y.code)) {
            cellInfo = g2Broad(y.code)
          } else if (y.rat == 6 && g4Broad.contains(y.code)) {
            cellInfo = g4Broad(y.code)
          }
          y.provinceCity = cellInfo
        })
        x
      }).flatMap((x: Trace) => {
        val all20list = new ListBuffer[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)]
        var thisCounty = ""
        var lastCounty = ""
        for (elem <- x.wheres) {
          breakable {
            thisCounty = elem.provinceCity
            if (thisCounty.isEmpty && lastCounty.isEmpty) {
              thisCounty = elem.provinceCity
              lastCounty = elem.provinceCity
              break()
            }
            if (!thisCounty.split("_", -1)(0).equals(lastCounty.split("_", -1)(0))) {
              val thisWhere: Where = elem
              var lastWhere: Where = elem
              if (x.wheres.indexOf(elem) != x.wheres.length - 1 && x.wheres.indexOf(elem) != 0 && x.wheres.indexOf(elem) != 1) {
                lastWhere = x.wheres(x.wheres.indexOf(elem) - 1)
              }
              // 进行处理
              // 日期,IMSI,手机号码,地市O点基站小区标识,O点基站归属地市编码,O点基站归属地市名称,O点基站所属场景,到达地市O点基站时间,地市O点出发时间,D点基站编号,D点基站归属地市编码,D点基站归属地市名称,D点基站所属场景,D点基站到达时间,D点地市最后离开基站时间,地市间OD距离（公里）,地市间OD时长（小时）,地市间OD速度,地市间OD出行方式,手机号码归属省,
              // 区县名称+“_”+区县编码+“_”+区县场景
              var oCountyName: String = ""
              if (lastWhere.provinceCity.split("_", -1).isDefinedAt(0)) {
                oCountyName = lastWhere.provinceCity.split("_")(0)
              }
              var oCountyNum: String = ""
              if (lastWhere.provinceCity.split("_", -1).isDefinedAt(1)) {
                oCountyNum = lastWhere.provinceCity.split("_")(1)
              }
              var oCountyScene: String = ""
              if (lastWhere.provinceCity.split("_", -1).isDefinedAt(2)) {
                oCountyScene = lastWhere.provinceCity.split("_")(2)
              }
              var dCountyName: String = ""
              if (thisWhere.provinceCity.split("_", -1).isDefinedAt(0)) {
                dCountyName = thisWhere.provinceCity.split("_")(0)
              }
              var dCountyNum: String = ""
              if (thisWhere.provinceCity.split("_", -1).isDefinedAt(1)) {
                dCountyNum = thisWhere.provinceCity.split("_")(1)
              }
              var dCountyScene: String = ""
              if (thisWhere.provinceCity.split("_", -1).isDefinedAt(2)) {
                dCountyScene = thisWhere.provinceCity.split("_")(2)
              }

              val distanceKM: Double = thisWhere.lLA.getDistance(lastWhere.lLA) / 1000
              val costSeconds: Double = (thisWhere.sTime - lastWhere.eTime) / 1000

              val speed: String = (thisWhere.lLA.getDistance(lastWhere.lLA) / costSeconds).formatted("%.2f")

              val costHour: String = (costSeconds / 3600).formatted("%.2f")

              all20list.append((x.date, x.imsi, "", lastWhere.code, oCountyNum, oCountyName, oCountyScene, lastWhere.sTime.toString, lastWhere.eTime.toString, thisWhere.code, dCountyNum, dCountyName, dCountyScene, thisWhere.sTime.toString, thisWhere.eTime.toString, distanceKM.toString, costHour, speed, "", ""))
              lastCounty = thisCounty
            }
          }
        }
        all20list
      })
      println(resultRDD.count())
      val rowRDD: RDD[Row] = resultRDD.map((x: (String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)) => Row(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9, x._10, x._11, x._12, x._13, x._14, x._15, x._16, x._17, x._18, x._19, x._20))
      println(rowRDD.count())
      val schema = "date_day,imsi,msisdn,O_cellID,O_county_code,O_county_name,O_cell_state,O_in_time,O_out_time,D_cellID,D_county_code,D_county_name,D_cell_state,D_in_time,D_out_time,OD_county_distance,OD_county_duration,OD_county_velocity,OD_county_type,home_province"
      val structType: StructType = StructType(schema.split(",").map((x: String) => StructField(x, StringType, nullable = true)))
      val dataFrame: DataFrame = hiveContext.createDataFrame(rowRDD, structType)
      dataFrame.createOrReplaceTempView("tmpTable")
      val insertSQL: String = s"insert overwrite table $outputDateBase.$outputTable partition(prodate='" + str_date + "') " +
        "select date_day,imsi,msisdn,O_cellID,O_county_code,O_county_name,O_cell_state,O_in_time,O_out_time,D_cellID,D_county_code,D_county_name,D_cell_state,D_in_time,D_out_time,OD_county_distance,OD_county_duration,OD_county_velocity,OD_county_type,home_province from tmpTable"
      println(insertSQL)
      hiveContext.sql(insertSQL)


      sc.stop()
      println("success!————区县OD完成" + str_date)
    }
  }
}
