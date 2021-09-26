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
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * @Author: whzHander 
 * @Date: 2021/5/25 9:31 
 * @Description:基于位置的客群识别模型1（出租车司机、公交车司机、快递人员）
 */
object customerGroupIdentify {
  def main(args: Array[String]): Unit = {
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
    val str_month = args.length match {
      case 1 => null
      case 2 => args(1)
      case _ => args(1)
    }
    val traceDistance = args.length match {
      case 1 => null
      case 2 => null
      case 3 => args(2)
      case _ => null
    }
    val houseCount = args.length match {
      case 1 => null
      case 2 => null
      case 3 => null
      case 4 => args(3)
      case _ => null
    }
    val avgRepetition = args.length match {
      case 1 => null
      case 2 => null
      case 3 => null
      case 4 => null
      case 5 => args(4)
      case _ => null
    }
    val avgServiceCountByHouse = args.length match {
      case 1 => null
      case 2 => null
      case 3 => null
      case 4 => null
      case 5 => null
      case 6 => args(5)
      case _ => null
    }
    val avgServiceCountByDate = args.length match {
      case 1 => null
      case 2 => null
      case 3 => null
      case 4 => null
      case 5 => null
      case 6 => null
      case 7 => args(6)
      case _ => null
    }
    val conf = new SparkConf().setAppName("customerGroupIdentify")
    ReadParam.readXML(conf, configPath)

    val sparkContext = new SparkContext(conf)
    val hiveContext = new HiveContext(sparkContext)

    val isLocal = conf.get("local")
    if ("1".equals(isLocal)) {
      conf.setMaster("local[*]")
    }

    val whereModelDatabase: String = conf.get("whereModelDatabase")
    val whereModelTable: String = conf.get("whereModelTable")
    val outputDataBase: String = conf.get("outputDataBase")
    val outputTable: String = conf.get("outputTable")

    //读取hdfs：YDY_ECI_DATA.txt
    val dataTextFile: BufferedReader = FileUtil.getBufferedReader(new Path(conf.get("G4Path")))
    val G2List = new mutable.HashMap[String, String]()
    val G4List = new mutable.HashMap[String, String]()
    var line: String = ""
    line = dataTextFile.readLine()
    while (line != null) {
      if (!line.contains("-")) {
        // eci，场景id+场景名称
        if (line.split("\\|").length >= 5 && !line.split("\\|", -1)(0).isEmpty && line.split("\\|", -1)(0).equals("YDY9")) {
          if (line.split("\\|")(2).contains("_")) {
            G2List.put(line.split("\\|")(2), line.split("\\|")(0) + "_" + line.split("\\|")(1))
          } else {
            G4List.put(line.split("\\|")(2), line.split("\\|")(0) + "_" + line.split("\\|")(1))
          }
        }
      }
      line = dataTextFile.readLine()
    }
    dataTextFile.close()
    println("G4List" + G4List.size)
    println("G2List" + G2List.size)
    val G4Broad: Broadcast[mutable.HashMap[String, String]] = sparkContext.broadcast(G4List)
    val G2Broad: Broadcast[mutable.HashMap[String, String]] = sparkContext.broadcast(G2List)


    //    读取离线位置模型：user_trace_merge_guizhou
    val readSQL = s"select imsi,starttime,endtime,staytime,longitude,latitude,code,grid,callno,imei,day_id from ${whereModelDatabase}.${whereModelTable} where day_id=${str_date}"
    val readInRDD: RDD[(String, String, String, String, String, String, String, String, String, String, String)] = hiveContext.sql(readSQL).rdd.map(x => {
      var imsi: String = ""
      if (!x.isNullAt(0) && !x.get(0).toString.isEmpty) {
        imsi = x.get(0).toString
      }
      var starttime: String = ""
      if (!x.isNullAt(1) && !x.get(1).toString.isEmpty) {
        starttime = x.get(1).toString
      }
      var endtime: String = ""
      if (!x.isNullAt(2) && !x.get(2).toString.isEmpty) {
        endtime = x.get(2).toString
      }
      var staytime: String = ""
      if (!x.isNullAt(3) && !x.get(3).toString.isEmpty) {
        staytime = x.get(3).toString
      }
      var longitude: String = ""
      if (!x.isNullAt(4) && !x.get(4).toString.isEmpty) {
        longitude = x.get(4).toString
      }
      var latitude: String = ""
      if (!x.isNullAt(5) && !x.get(5).toString.isEmpty) {
        latitude = x.get(5).toString
      }
      var code: String = ""
      if (!x.isNullAt(6) && !x.get(6).toString.isEmpty) {
        code = x.get(6).toString
      }
      var grid: String = ""
      if (!x.isNullAt(7) && !x.get(7).toString.isEmpty) {
        grid = x.get(7).toString
      }
      var callno: String = ""
      if (!x.isNullAt(8) && !x.get(8).toString.isEmpty) {
        callno = x.get(8).toString
      }
      var imei: String = ""
      if (!x.isNullAt(9) && !x.get(9).toString.isEmpty) {
        imei = x.get(9).toString
      }
      var day_id: String = ""
      if (!x.isNullAt(10) && !x.get(10).toString.isEmpty) {
        day_id = x.get(10).toString
      }
      (imsi, starttime, endtime, staytime, longitude, latitude, code, grid, callno, imei, day_id)
    })
    println("readInRDD:" + readInRDD.count())

    val whereRDD: RDD[(String, Where, String, String, String)] = readInRDD.filter(x => {
      val g2Broad: mutable.HashMap[String, String] = G2Broad.value
      val g4Broad: mutable.HashMap[String, String] = G4Broad.value
      g2Broad.contains(x._7) || g4Broad.contains(x._7)
    }).map(x => {
      //     1 imsi,2 starttime,3 endtime,4 staytime,5 longitude,6 latitude,7 code,8 grid,9 callno,10 imei,11 day_id
      val where = new Where()
      val imsi = x._1
      val callno = x._9
      val imei = x._10
      val day_id = x._11
      if (x._7.nonEmpty) {
        where.code = x._7
      }
      if (x._5.nonEmpty && x._6.nonEmpty) {
        where.lLA = new LLA(NumberUtils.toDouble(x._5), NumberUtils.toDouble(x._6))
      } else {
        where.lLA = new LLA(0D, 0D)
      }
      if (x._2.nonEmpty && x._3.nonEmpty) {
        where.sTime = NumberUtils.toLong(x._2)
        where.eTime = NumberUtils.toLong(x._3)
      }
      (imsi, where, callno, imei, day_id)
    })
    println("whereRDD:" + whereRDD.count())

    //    where的平滑处理：防止上一个where的etime大于当前where的stime
    val wheresRDD: RDD[(String, ListBuffer[Where], String, String, String)] = whereRDD.map(x => {
      var sortWheres = new ListBuffer[Where]()
      sortWheres.append(x._2)
      for (elem <- sortWheres) {
        if (sortWheres.indexOf(elem) != sortWheres.length - 1) {
          if (elem.eTime > sortWheres(sortWheres.indexOf(elem) + 1).sTime) {
            elem.eTime = Math.min(elem.eTime, sortWheres(sortWheres.indexOf(elem) + 1).sTime)
            sortWheres(sortWheres.indexOf(elem) + 1).sTime = Math.min(elem.eTime, sortWheres(sortWheres.indexOf(elem) + 1).sTime)
          }
        }
      }
      (x._1, sortWheres, x._3, x._4, x._5)
    })
    println("wheresRDD:" + wheresRDD.count())


    val traceRDD: RDD[(String, String, Trace)] = wheresRDD.map(x => {
      val trace = new Trace(LocType.SECTOR)
      x._2.foreach(y => {
        trace.loadAsLast(y)
      })
      trace.imsi = x._1
      trace.pohoneNum = ""
      trace.date = x._5
      (x._3, x._4, trace)
    })
    println("traceRDD:" + traceRDD.count())

    val anotherRDD: RDD[(Double, Int, Int, Int, Int, String, Trace)] = traceRDD.map(x => {
      //      var list :String=""
      var traceDistance: Double = 0 //门限1
      //      （code，经过次数）
      var house = new mutable.HashMap[String, Int]
      var houseCount: Int = 0 //门限2
      var avgRepetition: Int = 0 //门限3
      var repetition: Int = 0
      //      （code，通话次数）
      var serviceCount = new mutable.HashMap[String, Int]
      var avgServiceCountByHouse: Int = 0 //门限4
      var avgServiceCountByDate: Int = 0 //门限5
      for (elem <- x._3.wheres) {
        house.put(elem.code, 1)
        //        轨迹距离判断
        if (x._3.wheres.indexOf(elem) != x._3.wheres.length - 1) {
          traceDistance = traceDistance + elem.getDistance(x._3.wheres(x._3.wheres.indexOf(elem) + 1))
          //        经过不重复小区个数
          if (!house.contains(x._3.wheres(x._3.wheres.indexOf(elem) + 1).code)) {
            house.put(x._3.wheres(x._3.wheres.indexOf(elem) + 1).code, 1)
          }
          else {
            var countA: Int = house.get(elem.code) match {
              case Some(s) => s
              case None => 0
            }
            house.update(elem.code, countA + 1)
          }
        }

        //        有通话记录小区平均通话次数
        if (x._1.nonEmpty) {
          if (!serviceCount.contains(elem.code)) {
            serviceCount.put(elem.code, 1)
          }
          else {
            var countB: Int = serviceCount.get(elem.code) match {
              case Some(s) => s
              case None => 0
            }
            house.update(elem.code, countB + 1)
          }
        }
      }

      houseCount = house.size
      for (elem <- house) {
        repetition = repetition + elem._2
      }
      avgRepetition = repetition / houseCount

      for (elem <- serviceCount) {
        avgServiceCountByDate = avgServiceCountByDate + elem._2
      }
      if (serviceCount.nonEmpty) {
        avgServiceCountByHouse = avgServiceCountByDate / serviceCount.size
      }
      //      list=traceDistance.toString+"_"+ houseCount.toString+"_"+ avgRepetition.toString+"_"+avgServiceCountByHouse.toString+"_"+avgServiceCountByDate.toString
      //      （list，imei，trace）
      (traceDistance, houseCount, avgRepetition, avgServiceCountByHouse, avgServiceCountByDate, x._2, x._3)
    })
    println("anotherRDD:" + anotherRDD.count())


    val resultRDD: RDD[(String, String, String, String, String)] = anotherRDD.flatMap(x => {
      val all5list = new ListBuffer[(String, String, String, String, String)]
      //          (traceDistance,houseCount,avgRepetition,avgServiceCountByHouse,avgServiceCountByDate)
      if (x._1 > 300 && x._2 > 2 && x._3 < 2 && x._5 > 2) {
        all5list.append((x._7.imsi, "", x._6, "出租车司机", str_date))
      }
      if (x._1 > 300 && x._2 > 2 && x._3 > 2 && x._4 < 2 && x._5 < 2) {
        all5list.append((x._7.imsi, "", x._6, "公交车司机", str_date))
      }
      if (x._1 > 300 && x._2 > 2 && x._3 > 2 && x._4 > 2 && x._5 > 2) {
        all5list.append((x._7.imsi, "", x._6, "快递人员", str_date))
      }
      all5list
    })
    println("resultRDD:" + resultRDD.count())

    val rowRDD: RDD[Row] = resultRDD.map(x => Row(x._1, x._2, x._3, x._4, x._5))

    val schema = "imsi,msisdn,imei,crowd_name,stat_date"
    val structType: StructType = StructType(schema.split(",").map(x => StructField(x, StringType, true)))
    val dataFrame: DataFrame = hiveContext.createDataFrame(rowRDD, structType)
    dataFrame.createOrReplaceTempView("tmpTable")
    val insertSQL: String = s"insert overwrite table ${outputDataBase}.${outputTable} partition(prodate='${str_date}') " +
      "select imsi,msisdn,imei,crowd_name,stat_date from tmpTable"
    println(insertSQL)
    hiveContext.sql(insertSQL)


    sparkContext.stop()
    println("success!————客群识别1完成" + str_date)
  }
}
