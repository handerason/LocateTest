package com.bonc.ServerTest

import com.bonc.lla.LLA
import com.bonc.location.LocType
import com.bonc.location.LocType.LocType
import com.bonc.trace.{Trace, Where}
import com.bonc.user_locate.ReadParam
import com.bonc.user_locate.utils.{FileUtil, LocateUtils, getGeoHash}
import mrLocateV2.bsparam.{Cell, EarfcnPciUnitedKey}
import org.apache.commons.lang.math.NumberUtils
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import java.io.BufferedReader
import java.text.SimpleDateFormat
import java.util
import java.util.TimeZone

import org.apache.spark.sql.Row

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}

/**
 * 基站出入模型
 */
object userCellFlowModel {
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

    // 将区县信息做成HashMap的结构（eci，区县）
    // 加载4G工参,格式：(eci,区县)，到时候这个参数处理一下
    val G4Reader: BufferedReader = FileUtil.getBufferedReader(new Path(conf.get("G4Path")))
    val G4List = new mutable.HashMap[String, String]()
    var G4: String = ""
    G4 = G4Reader.readLine()
    while (G4 != null) {
      if (!G4.contains("-")) {
        G4List.put(G4.split("\t")(0), G4.split("\t")(7))
      }
      G4 = G4Reader.readLine()
    }
    G4Reader.close()
    println("G4List" + G4List.size)
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
      }).filter(x => {
      var bool = true
      breakable{
        x.wheres.foreach(y => {
          if (y.lLA.longitude <0 || y.lLA.latitude <0 || y.lLA.longitude > 180 || y.lLA.latitude > 180){
            bool = false
            break()
          }
        })
      }
      bool
    })

    val enbidRDD: RDD[Trace] = traceRDD.map(x => {
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
      //            if (whereList.length == 1){
      //              val where = new Where()
      //              val sortTmpWhere: ListBuffer[Where] = tmpWheres.sortBy(_.sTime)
      //              // 属性赋值
      //              where.sTime = sortTmpWhere.head.sTime
      //              where.eTime = sortTmpWhere.last.eTime
      //              where.code = choseEnbid
      //
      //              // 经纬度
      //              var lonAll = 0D
      //              var latAll = 0D
      //              for (elem <- sortTmpWhere) {
      //                lonAll += elem.lLA.longitude
      //                latAll += elem.lLA.latitude
      //              }
      //              lonAll = lonAll / sortTmpWhere.size
      //              latAll = latAll / sortTmpWhere.size
      //              where.lLA = new LLA(lonAll, latAll)
      //              wheres.append(where)
      //
      //            }
      //            break()
      //          } else {
      //            // 遍历其后元素
      //            if (choseEnbid == elem.code) {
      //              tmpWheres.append(elem)
      //              break()
      //            }
      //            if (choseEnbid != elem.code || whereList.indexOf(elem) == whereList.length - 1) {
      //              val where = new Where()
      //              val sortTmpWhere: ListBuffer[Where] = tmpWheres.sortBy(_.sTime)
      //              // 属性赋值
      //              where.sTime = sortTmpWhere.head.sTime
      //              where.eTime = sortTmpWhere.last.eTime
      //              where.code = choseEnbid
      //              // 经纬度
      //              var lonAll = 0D
      //              var latAll = 0D
      //              for (elem <- sortTmpWhere) {
      //                lonAll += elem.lLA.longitude
      //                latAll += elem.lLA.latitude
      //              }
      //              lonAll = lonAll / sortTmpWhere.size
      //              latAll = latAll / sortTmpWhere.size
      //              where.lLA = new LLA(lonAll, latAll)
      //              wheres.append(where)
      //              tmpWheres.clear()
      //              choseEnbid = elem.code
      //            }
      //          }
      //        }
      //      }
      val trace = new Trace(x.imsi, x.traceType)
      trace.date = x.date
      whereList.foreach(z => {
        trace.loadAsLast(z)
      })
      trace
    })

    val resultRDD: RDD[(String, String, String, String, Double, Double, String, String, String, Long, Long)] = enbidRDD.flatMap(x => {
      // 手机号 基站小区标识 基站经度 基站纬度 区县编码 进入基站时间 离开基站时间
      val listBuffer = new ListBuffer[(String, String, String, String, Double, Double, String, String, String, Long, Long)]
      x.wheres.foreach(y => {
        // 获取基站的区县编码
        val G4Map: mutable.HashMap[String, String] = G4Broad.value
        val district: String = G4Map.getOrElse(y.code, "")
        // 获取空间位置编码
        val geoCode: String = getGeoHash.encode(y.lLA.longitude, y.lLA.latitude)

        if (!y.code.contains("-")) {
          // imsi 手机号 imei 基站小区标识 基站经度 基站纬度 空间地理编码 国家码 区县编码 进入基站时间 离开基站时间
          listBuffer.append((x.imsi.split("\t")(1), x.imsi.split("\t")(0), x.imsi.split("\t")(2), y.code, y.lLA.longitude, y.lLA.latitude, geoCode, "CHN", district, y.sTime, y.eTime))
        }

      })
      listBuffer
    })


    val path = new Path(outputPath)
    val fs = path.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(path)) {
      fs.delete(path, true)
      System.out.println(outputPath + "已存在，删除")
    }
    resultRDD.map(x => {
      Row(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9, x._10, x._11)
    }).saveAsTextFile(outputPath)

    sc.stop()
    println("success!————基站出入模型" + date)
  }


}
