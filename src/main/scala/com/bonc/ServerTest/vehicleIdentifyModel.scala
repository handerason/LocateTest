package com.bonc.ServerTest

import com.bonc.lla.LLA
import com.bonc.location.LocType
import com.bonc.trace.{Trace, Where}
import com.bonc.user_locate.ReadParam
import com.bonc.user_locate.utils.{FileUtil, LocateUtils}
import mrLocateV2.bsparam.{Cell, EarfcnPciUnitedKey}
import org.apache.commons.lang.math.NumberUtils
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import java.io.BufferedReader
import java.text.SimpleDateFormat
import java.util
import java.util.TimeZone

import org.apache.spark.sql.Row

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}

/**
 * @deprecated 出行交通工具识别
 * @author opslos
 * @date 2021/1/4 21:00:03
 */
object vehicleIdentifyModel {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("com").setLevel(Level.WARN)

    if (args.length < 2) {
      println("args must be at least 2")
      System.exit(1)
    }
    val configPath = args(0) // 配置文件路径，与jar包相同目录
    val date = args(1) // 日期

    val conf = new SparkConf()
    ReadParam.readXML(conf, configPath)
    conf.set("spark.sql.shuffle.partitions", "1000")
    conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    conf.set("date", date)
    conf.set("spark.yarn.queue", conf.get("queuename"))
    conf.setAppName("user_trace" + date)
    conf.set("spark.driver.maxResultSize", "20g")
    // 使用kryo序列化
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array[Class[_]](classOf[Nothing]))

    //判断是否启用本地模式
    val isLocal = conf.get("local")
    if ("1".equals(isLocal)) {
      conf.setMaster("local[*]")
    }


    val sc = new SparkContext(conf) // 创建SparkContext

    var outputPath = conf.get("outputpath")
    var inputpath = conf.get("inputpath")

    val cellMap = new util.HashMap[java.lang.Long, Cell]()
    val eciMap = new util.HashMap[Integer, util.HashMap[EarfcnPciUnitedKey, java.lang.Long]]
    LocateUtils.readBs2Cache(sc, isLocal, conf.get("site", ""), cellMap, eciMap)
    System.out.println("cellMap---" + cellMap.size())
    System.out.println("eciMap---" + eciMap.size())

    // 这里将所有的规定好的基站小区范围，高铁站和机场的基站列表
    // 根据提供的全量的去处理，最后得到的数据是（机场或者高铁站名,eci列表）
    val G4Reader: BufferedReader = FileUtil.getBufferedReader(new Path(conf.get("trainStation")))
    val G4List = new mutable.HashMap[String, String]()
    var G4: String = ""
    G4 = G4Reader.readLine()
    while (G4 != null) {
      if (G4.contains("贵阳北")) {
        G4List.put(G4.split(",")(1), "GYTrainStation")
      } else {
        G4List.put(G4.split(",")(1), "ZYTrainStation")
      }
      G4 = G4Reader.readLine()
    }
    G4Reader.close()
    println("G4List:" + G4List.size)
    G4List.foreach(println(_))
    val G4Broad: Broadcast[mutable.HashMap[String, String]] = sc.broadcast(G4List)


    // 1.2将工参信息广播
    val bd_cellMap = sc.broadcast(cellMap)

    val traceRDD = sc.textFile(inputpath).filter(x => {
      !x.contains("unknow") && !x.contains("null") && !x.contains("\\N")
    }).filter(x => {
      x.split("\t").length >= 8
    }).mapPartitions(iter => {
      val list = new ListBuffer[(String, Where)]
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      format.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
      var mapCellMap = bd_cellMap.value
      while (iter.hasNext) {
        val line = iter.next().toString
        var arr = line.split("\t")
        val date = arr(0)
        val phone = arr(1)
        val imei: String = arr(4)
        val eci = NumberUtils.toLong(arr(2))
        val imsi = arr(3)
        val cell: Cell = mapCellMap.get(eci)
        if (cell != null && cell.getXYZ != null) {
          val longitude = cell.getXYZ.toLB(105).getLongtitude
          val latitude = cell.getXYZ.toLB(105).getLatitude
          val starttime = format.parse(arr(5)).getTime
          val endtime = format.parse(arr(7)).getTime

          val where = new Where
          where.lLA = new LLA(longitude, latitude)
          where.sTime = starttime
          where.eTime = endtime
          where.code = eci.toString
          list.append((phone + "\t" + imsi + "\t" + imei + "\t" + date, where))
        }
      }
      list.toIterator
    }).groupByKey()
      .map((x: (String, Iterable[Where])) => {

        val recordList = x._2.toList
        val sortRecordList = recordList.sortBy(_.sTime).sortBy(_.eTime)
        val trace = new Trace(LocType.SECTOR)
        trace.imsi = x._1
        trace.date = x._1.split("\t")(3)
        trace.createByWhere(sortRecordList)

        trace
      }).filter(_.imsi.split("\t").length == 4)

    println(traceRDD.count())

    val mergeWhereTrace: RDD[Trace] = traceRDD.map(x => {
      val G4Map: mutable.HashMap[String, String] = G4Broad.value
      x.wheres.foreach(y => {
        if (G4Map.keys.toList.contains(y.code)) {
          y.code = G4Map(y.code)
        }
      })
      // 上边识别完成之后进行合并，将交通枢纽的数据进行合并
      val whereList: List[Where] = x.wheres.toList.sortBy(_.sTime)
      //      val wheres = new ListBuffer[Where]
      //      val tmpWheres = new ListBuffer[Where]
      //      var choseEnbid = ""
      //      for (elem <- whereList) {
      //        breakable {
      //          // index为0，一开始
      //          if (whereList.indexOf(elem) == 0) {
      //            choseEnbid = elem.code
      //            tmpWheres.append(elem)
      //            break()
      //          } else {
      //            // 遍历其后元素
      //            if (choseEnbid == elem.code) {
      //              tmpWheres.append(elem)
      //              break()
      //            } else if (choseEnbid != elem.code || whereList.indexOf(elem) == whereList.length - 1) {
      //              val where = new Where()
      //              val sortTmpWhere: ListBuffer[Where] = tmpWheres.sortBy(_.sTime)
      //              // 属性赋值
      //              where.sTime = sortTmpWhere.head.sTime
      //              where.eTime = sortTmpWhere.last.eTime
      //              where.code = choseEnbid
      //
      //              wheres.append(where)
      //              tmpWheres.clear()
      //              choseEnbid = elem.code
      //            }
      //          }
      //        }
      //      }
      val trace = new Trace(x.imsi, x.traceType)
      trace.date = x.date
      whereList.foreach(y => {
        trace.loadAsLast(y)
      })
      trace
    }).filter(x => {
      // 过滤掉没有出行信息的Trace
      val G4Map: mutable.HashMap[String, String] = G4Broad.value
      val places: List[String] = G4Map.values.toList
      val wheres = new ListBuffer[Where]
      x.wheres.foreach(y => {
        if (places.contains(y.code)) {
          wheres.append(y)
        }
      })
      wheres.distinct.length >= 2
    })

    println(mergeWhereTrace.count())

    val resultRDD: RDD[(Long, String, String, String, String, String, String, Double, Double)] = mergeWhereTrace.map(x => {
      val trafficHub: List[String] = G4Broad.value.values.toList
      val imsi: String = x.imsi.split("\t")(1)
      val phone: String = x.imsi.split("\t")(0)
      val imei: String = x.imsi.split("\t")(2)

      var endHub: String = "1"
      var startHub: String = "1"
      var timeStamp = 0L
      var duration = 0D
      var mileage = 0D

      // 获取两站的wheres
      val resultWhere = new ListBuffer[Where]
      val wheres: List[Where] = x.wheres.toList.sortBy(_.sTime)
      for (elem <- wheres) {
        if (trafficHub.contains(elem.code)) {
          resultWhere.append(elem)
        }
      }

      // 按照时间排序，获得出发点和终点
      val chosenWhere: ListBuffer[Where] = resultWhere.sortBy(_.sTime)
      if (chosenWhere.nonEmpty){
        // 出发点
        val startWhere: Where = chosenWhere.head
        timeStamp = startWhere.eTime
        startHub = startWhere.code
        // 终点
        val arriverWheres: ListBuffer[Where] = chosenWhere.filter(!_.code.equals(startWhere.code))
        if (arriverWheres.nonEmpty){
          val arriveWhere: Where = arriverWheres.minBy(_.sTime)
          endHub = arriveWhere.code
          // 时间间隔和距离
          duration = arriveWhere.sTime - startWhere.eTime
          mileage = startWhere.getDistance(arriveWhere)
        }
      }

      (timeStamp, imsi, phone, imei, startHub, endHub, "高铁", mileage, duration)
    }).filter(x => {
      x._1 != 0L && x._2 != "" && x._3 != "" && x._4 != "" && x._5 != "" && x._6 != ""  && x._8 != 0D && x._9 != 0D
    })

    val path = new Path(outputPath)
    val fs = path.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(path)) {
      fs.delete(path, true)
      System.out.println(outputPath + "已存在，删除")
    }
    // 将这个结果先进行保存
    resultRDD.map(x => {
      Row(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9)
    }).repartition(1).saveAsTextFile(outputPath)


    sc.stop()
    println("success!————交通工具识别模型" + date)

  }
}















