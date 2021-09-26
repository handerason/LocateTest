package com.bonc.ServerTest

import java.io.BufferedReader
import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date, TimeZone}

import com.bonc.lla.LLA
import com.bonc.location.LocType
import com.bonc.trace.{Trace, Where}
import com.bonc.user_locate.ReadParam
import com.bonc.user_locate.utils.{FileUtil, LocateUtils}
import mrLocateV2.bsparam.{Cell, EarfcnPciUnitedKey}
import org.apache.commons.lang.math.NumberUtils
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * 行政区流入流出模型
 *
 * @author opslos
 * @date 2021/1/4 17:14:44
 */
object predefinedAreaModel {
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

    val hourFormat = new SimpleDateFormat("yyyyMMdd HH:00:00")

    //判断是否启用本地模式
    val isLocal = conf.get("local")
    if ("1".equals(isLocal)) {
      conf.setMaster("local[*]")
    }
    val isTest = conf.getBoolean("isTest", false)
    val isMainCellFilter = conf.getBoolean("mr_isMainCellFilter", false)
    val database = conf.get("database", "default")

    val sc = new SparkContext(conf) // 创建SparkContext
    val hc = new HiveContext(sc) // 创建HiveContext

    var outputPath = conf.get("outputpath")
    var inputpath = conf.get("inputpath")

    val startDate = new Date()
    val startTime: Long = startDate.getTime

    val cellMap = new util.HashMap[java.lang.Long, Cell]()
    val eciMap = new util.HashMap[Integer, util.HashMap[EarfcnPciUnitedKey, java.lang.Long]]
    LocateUtils.readBs2Cache(sc, isLocal, conf.get("site", ""), cellMap, eciMap)
    System.out.println("cellMap---" + cellMap.size())
    System.out.println("eciMap---" + eciMap.size())

    // 将区县信息做成HashMap的结构（eci，行政区）
    // 加载4G工参,格式：(eci,行政区)，到时候这个参数处理一下
    val G4Reader: BufferedReader = FileUtil.getBufferedReader(new Path(conf.get("G4Path")))
    val G4List = new mutable.HashMap[String, String]()
    var G4: String = ""
    G4 = G4Reader.readLine()
    while (G4 != null) {
      G4List.put(G4.split(",")(1),"GuiYangRailWayStation")
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
      x.split("\t").length >= 5
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
      })

    // 自定义区域的人口测算
    val combineWhereTrace: RDD[Trace] = traceRDD.map(x => {
      val G4: mutable.HashMap[String, String] = G4Broad.value
      x.wheres.foreach(y => {
        y.code = G4.getOrElse(y.code,"")
      })
      // 进行where的合并
      val whereList: List[Where] = x.wheres.toList.sortBy(_.sTime)
      val trace = new Trace(x.imsi, x.traceType)
      whereList.foreach(y => {
        trace.loadAsLast(y)
      })
      trace
    })

    val countRDD: RDD[(String, String, String, String, String)] = combineWhereTrace.flatMap(x => {
      // 时间段，行政区，保持人的imsi，流入人的imsi，流出人的imsi
      val resultList = new ListBuffer[(String, String, String, String, String)]

      x.wheres.toList.sortBy(_.sTime).foreach(y => {
        val sHourStamp: String = hourFormat.format(new Date(y.sTime))
        val eHourStamp: String = hourFormat.format(new Date(y.eTime))
        val sHour: Int = NumberUtils.toInt(sHourStamp.substring(9, 11))
        val eHour: Int = NumberUtils.toInt(eHourStamp.substring(9, 11))
        if (sHourStamp.equals(eHourStamp)) {
          resultList.append((sHourStamp, y.code, "", x.imsi, ""))
          resultList.append((eHourStamp, y.code, "", "", x.imsi))
        } else if (sHour < eHour) {
          val diff: Int = eHour - sHour
          // 驻留时间
          for (i <- Range(1, diff)) {
            val calendar: Calendar = Calendar.getInstance()
            calendar.setTime(new Date(y.sTime))
            calendar.add(Calendar.HOUR, i)
            val upTime: Date = calendar.getTime
            val upHour: String = hourFormat.format(upTime)
            resultList.append((upHour, y.code, x.imsi, "", ""))
          }
          // 进入
          resultList.append((sHourStamp, y.code, "", x.imsi, ""))
          // 离开
          resultList.append((eHourStamp, y.code, "", "", x.imsi))
        }
      })
      resultList
    }).filter(x => {
      val list: List[String] = G4Broad.value.values.toList
      list.contains(x._2)
    })

    val resultRDD: RDD[(String, String, String, String, Int, Int, Int, Int,String)] = countRDD.map(x => {
      val str: String = x._1 + "\t" + x._2
      (str, x._3, x._4, x._5)
    }).groupBy(_._1).map(x => {
      val strings: Array[String] = x._1.split("\t")
      val hourStamp: String = strings(0)
      val code: String = strings(1)
      var areaID = ""
      if (code.equals("贵阳")){
        areaID = "0851"
      }else if (code.equals("遵义")){
        areaID = "0852"
      }
      val stay = new ListBuffer[String]
      val inFlow = new ListBuffer[String]
      val outFlow = new ListBuffer[String]
      for (elem <- x._2) {
        stay.append(elem._2)
        inFlow.append(elem._3)
        outFlow.append(elem._4)
      }
      val stayCount: Int = stay.count(y => y.nonEmpty)
      val inFlowCount: Int = inFlow.count(y => y.nonEmpty)
      val outFlowCount: Int = outFlow.count(y => y.nonEmpty)
      val totalNum: Int = stayCount + inFlowCount - outFlowCount
      (hourStamp, areaID, code, "", totalNum, stayCount, inFlowCount, outFlowCount, hourStamp.split(" ")(0))
    }).filter(x => {
      x._5 > 0 && x._6 > 0 && x._7 > 0 && x._8 > 0
    })

    val path = new Path(outputPath)
    val fs = path.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(path)) {
      fs.delete(path, true)
      System.out.println(outputPath + "已存在，删除")
    }
    // 将这个结果先进行保存
    resultRDD.map(x => {
      Row(x._1,x._2,x._3,x._4,x._5,x._6,x._7,x._8,x._9)
    }).repartition(1).saveAsTextFile(outputPath)

    val endDate = new Date()
    val endTime: Long = endDate.getTime

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val duartion: Long = (endTime - startTime) / 1000

    val second: Long = duartion / 60
    val minute: Long = duartion / 60 % 60
    val hour: Long = duartion / 3600 % 60;

    println("程序开始时间:"+dateFormat.format(startDate))
    println("程序结束时间:"+dateFormat.format(endDate))
    println("程序运行时间:"+hour+":"+minute+":"+second)

    sc.stop()
    println("success!————自定义区域流入流出模型" + date)

  }
}























