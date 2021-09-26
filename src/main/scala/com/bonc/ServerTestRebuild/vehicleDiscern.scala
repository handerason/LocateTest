package com.bonc.ServerTestRebuild

import java.io.BufferedReader

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
 * @Date: 2021/8/27 10:13 
 * @Description:
 * This code sucks, you know it and I know it.
 * Move on and call me an idiot later.
 * If this code works, it was written by Wang Hengze.
 * If not, I don't know who wrote it.
 */
object vehicleDiscern {
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

    val conf: SparkConf = new SparkConf().setAppName("vehicleDiscern")
    ReadParam.readXML(conf, configPath)

    val isLocal: String = conf.get("local")
    if ("1".equals(isLocal)) {
      conf.setMaster("local[*]")
    }

    val inputDataBase: String = conf.get("inputDataBase")
    val inputTable: String = conf.get("inputTable")
    val outputDataBase: String = conf.get("outputDataBase")
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
      if (!gc.contains("-") && !gc.split("\\|", -1)(0).isEmpty) {
        // （eci，场景名称）
        //        选取场景为交通枢纽的数据
        if (gc.split("\\|", -1)(0).equals("YDY2") || gc.split("\\|", -1)(0).equals("YDY3")) {
          if (gc.split("\\|")(2).contains("_")) {
            G2List.put(gc.split("\\|")(2), gc.split("\\|")(1))
          } else {
            G4List.put(gc.split("\\|")(2), gc.split("\\|")(1))
          }
        }
      }
      gc = gcReader.readLine()
    }
    gcReader.close()
    println("G4List:" + G4List.size)
    println("G2List:" + G2List.size)
    val G4Broad: Broadcast[mutable.HashMap[String, String]] = sc.broadcast(G4List)
    val G2Broad: Broadcast[mutable.HashMap[String, String]] = sc.broadcast(G2List)

    //读取离线位置模型（手机号和imei暂时为空）
    val readSQL = s"select imsi,starttime,endtime,staytime,longitude,latitude,code,callno,imei,day_id from $inputDataBase.$inputTable where day_id='$str_date'"
    val inputRDD: RDD[(String, String, String, String, String, String, String, String, String, String)] = hiveContext.sql(readSQL)
      .rdd
      .map((x: Row) => {
        val imsi: String = x.get(0).toString
        val starttime: String = x.get(1).toString
        val endtime: String = x.get(2).toString
        val staytime: String = x.get(3).toString
        val longitude: String = x.get(4).toString
        val latitude: String = x.get(5).toString
        val code: String = x.get(6).toString
        val callno: String = x.get(7).toString
        val imei: String = x.get(8).toString
        val day_id: String = x.get(9).toString
        (imsi, starttime, endtime, staytime, longitude, latitude, code, callno, imei, day_id)
      })
    println("inputRDD" + inputRDD.count())
    inputRDD.take(5).foreach {
      println
    }


    val traceRDD: RDD[Trace] = inputRDD.filter((x: (String, String, String, String, String, String, String, String, String, String)) => {
      val g2Broad: mutable.HashMap[String, String] = G2Broad.value
      val g4Broad: mutable.HashMap[String, String] = G4Broad.value
      g2Broad.contains(x._7) || g4Broad.contains(x._7)
    }).map((x: (String, String, String, String, String, String, String, String, String, String)) => {
      val where = new Where
      //imsi
      val imsi: String = x._1
      // rat
      if (x._7.contains("_")) {
        where.rat = 2
      } else {
        where.rat = 6
      }
      //eci
      if (where.rat == 2 && x._7.split("_").length == 2) {
        where.code = NumberUtils.toLong(x._7.split("_")(0)) * 100000L + NumberUtils.toLong(x._7.split("_")(1)).toString
      } else {
        where.code = NumberUtils.toLong(x._7).toString
      }
      // LLA
      where.lLA = new LLA(NumberUtils.toDouble(x._5), NumberUtils.toDouble(x._6))
      // sTime
      where.sTime = TimeStampUtils.tranTimeToLong(x._2)
      // eTime
      where.eTime = TimeStampUtils.tranTimeToLong(x._3)
      // phone
      val phone: String = x._8
      // imei
      val imei: String = x._9

      (imsi + "\t" + phone + "\t" + imei, where)
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
      }).map((x: (String, List[Where])) => {
      val trace = new Trace(LocType.SECTOR)
      x._2.foreach((y: Where) => {
        trace.loadAsLast(y)
      })
      trace.imsi = x._1.split("\t", -1)(0)
      //      电话号 imei
      trace.pohoneNum = x._1.split("\t", -1)(1) + "\t" + x._1.split("\t", -1)(2)
      trace.date = str_date
      trace
    })
    println("traceRDD:" + traceRDD.count())
    traceRDD.take(5).foreach {
      println
    }

    val resultRDD: RDD[(String, String, String, String, String, String, String, String, String)] = traceRDD.map((x: Trace) => {
      val g2Broad: mutable.HashMap[String, String] = G2Broad.value
      val g4Broad: mutable.HashMap[String, String] = G4Broad.value
      x.wheres.foreach((y: Where) => {
        //        场景名称
        var source: String = ""
        if (y.rat == 2 && g2Broad.contains(y.code)) {
          source = g2Broad(y.code)
        } else if (y.rat == 6 && g4Broad.contains(y.code)) {
          source = g4Broad(y.code)
        }
        y.source = source
      })
      x
    }).flatMap((x: Trace) => {
      val resultList = new ListBuffer[(String, String, String, String, String, String, String, String, String)]
      var thisScene = ""
//      var lastScene = ""
      var startScene = ""
      var startElem = 0
      //      var thisEci = ""
      //      var lastEci = ""
      for (elem <- x.wheres) {
        breakable {
          thisScene = elem.source
          if (startScene.isEmpty) {
            startScene = elem.source
            break()
          }
          if (!thisScene.equals(startScene) || x.wheres.indexOf(elem)==x.wheres.length-1) {
            var thisWhere: Where = elem
            val startWhere: Where = x.wheres(startElem)
            if (x.wheres.indexOf(elem) != x.wheres.length - 1 && x.wheres.indexOf(elem) != 0 && x.wheres.indexOf(elem) != 1) {
              thisWhere = x.wheres(x.wheres.indexOf(elem) - 1)
            }
            //            出发交通枢纽
            val start_transportation: String = startWhere.code
            //            到达交通枢纽
            val end_transportation: String = thisWhere.code
            //            交通工具类型
            val trans_type: String = thisWhere.source.substring(0, 2)
            //            出行里程
            val distanceKM: Double = thisWhere.lLA.getDistance(startWhere.lLA) / 1000
            //            出行时长
            val costSeconds: Double = (thisWhere.sTime - startWhere.eTime) / 1000
            val costHour: String = (costSeconds / 3600).formatted("%.2f")

            if (distanceKM > 20 || costSeconds > 600) {
              resultList.append((str_date, x.imsi, x.pohoneNum.split("\t", -1)(0), x.pohoneNum.split("\t", -1)(1), start_transportation, end_transportation, trans_type, distanceKM.toString, costHour))
            }

            startElem = x.wheres.indexOf(elem)
            startScene = elem.source
          }

        }
      }
      //        //当场景相同但是eci不相同时，判断为进行了一次交通工具出行
      //        breakable {
      //          thisScene = elem.source
      //          //          thisEci = elem.code
      //          if (thisScene.isEmpty && lastScene.isEmpty) {
      //            thisScene = elem.source
      //            //            thisEci = elem.code
      //            lastScene = elem.source
      //            startScene = elem.source
      //            //            lastEci = elem.code
      //            break()
      //          }
      //          if (!thisScene.equals(lastScene)) {
      //
      //          }
      //          else {
      //            startScene
      //          }
      //          if (thisEci.equals(lastEci)) {
      //            val thisWhere: Where = elem
      //            var lastWhere: Where = elem
      //            if (x.wheres.indexOf(elem) != x.wheres.length - 1 && x.wheres.indexOf(elem) != 0 && x.wheres.indexOf(elem) != 1) {
      //              lastWhere = x.wheres(x.wheres.indexOf(elem) - 1)
      //            }
      //            //            出发交通枢纽
      //            val start_transportation: String = lastWhere.code
      //            //            到达交通枢纽
      //            val end_transportation: String = thisWhere.code
      //            //            交通工具类型
      //            val trans_type: String = thisWhere.source.substring(0, 2)
      //            //            出行里程
      //            val distanceKM: Double = thisWhere.lLA.getDistance(lastWhere.lLA) / 1000
      //            //            出行时长
      //            val costSeconds: Double = (thisWhere.sTime - lastWhere.eTime) / 1000
      //            val costHour: String = (costSeconds / 3600).formatted("%.2f")
      //
      //            resultList.append((str_date, x.imsi, x.pohoneNum.split("\t", -1)(0), x.pohoneNum.split("\t", -1)(1), start_transportation, end_transportation, trans_type, distanceKM.toString, costHour))
      //
      //            //            lastEci = thisEci
      //            lastScene = thisScene
      //
      //          }
      //        }
      //      }
      resultList
    })
    println("resultRDD:" + resultRDD.count())


    val rowRDD: RDD[Row] = resultRDD.map((x: (String, String, String, String, String, String, String, String, String)) => Row(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9))
    println(rowRDD.count())
    val schema = "timeStamp,imsi,phone,imei,start_transportation,end_transportation,trans_type,mileage,duration"
    val structType: StructType = StructType(schema.split(",").map((x: String) => StructField(x, StringType, nullable = true)))
    val dataFrame: DataFrame = hiveContext.createDataFrame(rowRDD, structType)
    dataFrame.createOrReplaceTempView("tmpTable")
    val insertSQL: String = s"insert overwrite table $outputDataBase.$outputTable partition(day_id='" + str_date + "') " +
      "select timeStamp,imsi,phone,imei,start_transportation,end_transportation,trans_type,mileage,duration from tmpTable"
    println(insertSQL)
    hiveContext.sql(insertSQL)

    println("success!————交通工具识别模型" + str_date)

    sc.stop()
  }
}
