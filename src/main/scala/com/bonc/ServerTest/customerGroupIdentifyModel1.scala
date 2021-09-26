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
import java.{lang, util}
import java.util.{Date, TimeZone}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}

/**
 * @deprecated 出行客户群体识别；出租车，公交车，快递人员的用Trace轨迹来识别
 * @author opslos
 * @date 2021/1/4 20:53:39
 */
object customerGroupIdentifyModel1 {
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

    // 获取参数
    // 重复小区门限
    val sameCellThreshold: Int = NumberUtils.toInt(conf.get("sameCellThreshold"))
    // 平均重复次数
    val avgSameCellTimeMin: Int = NumberUtils.toInt(conf.get("avgSameCellTimeMin"))
    val avgSameCellTimeMax: Int = NumberUtils.toInt(conf.get("avgSameCellTimeMax"))

    //判断是否启用本地模式
    val isLocal: String = conf.get("local")
    if ("1".equals(isLocal)) {
      conf.setMaster("local[*]")
    }

    val sc = new SparkContext(conf) // 创建SparkContext

    var outputPath: String = conf.get("outputpath")
    var inputPath: String = "/tenantDFGX/data/MR/bonc_2021010[1-9]/"


    val cellMap = new util.HashMap[java.lang.Long, Cell]()
    val eciMap = new util.HashMap[Integer, util.HashMap[EarfcnPciUnitedKey, java.lang.Long]]
    LocateUtils.readBs2Cache(sc, isLocal, conf.get("site", ""), cellMap, eciMap)
    System.out.println("cellMap---" + cellMap.size())
    System.out.println("eciMap---" + eciMap.size())

    //    // 这里将所有指定的学校区域
    //    val schoolReader: BufferedReader = FileUtil.getBufferedReader(new Path(conf.get("schoolPath")))
    //    val schoolMap = new mutable.HashMap[String, List[String]]()
    //    var school: String = ""
    //    school = schoolReader.readLine()
    //    while (school != null) {
    //      schoolMap.put(school.split("\t")(0), school.split("\t")(1).split(",").toList)
    //      school = schoolReader.readLine()
    //    }
    //    schoolReader.close()
    //    println("schoolList" + schoolMap.size)
    //    val schoolBroad: Broadcast[mutable.HashMap[String, List[String]]] = sc.broadcast(schoolMap)
    //
    //    // 这里将所有指定的医院区域
    //    val hospitalReader: BufferedReader = FileUtil.getBufferedReader(new Path(conf.get("hospitalPath")))
    //    val hospitalMap = new mutable.HashMap[String, List[String]]()
    //    var hospital: String = ""
    //    hospital = hospitalReader.readLine()
    //    while (hospital != null) {
    //      hospitalMap.put(hospital.split("\t")(0), hospital.split("\t")(1).split(",").toList)
    //      hospital = hospitalReader.readLine()
    //    }
    //    hospitalReader.close()
    //    println("hospitalList" + hospitalMap.size)
    //    val hospitalBroad: Broadcast[mutable.HashMap[String, List[String]]] = sc.broadcast(hospitalMap)
    //
    //    // 这里将所有指定的商务区域
    //    val businessReader: BufferedReader = FileUtil.getBufferedReader(new Path(conf.get("businessPath")))
    //    val businessMap = new mutable.HashMap[String, List[String]]()
    //    var business: String = ""
    //    business = businessReader.readLine()
    //    while (business != null) {
    //      businessMap.put(business.split("\t")(0), business.split("\t")(1).split(",").toList)
    //      business = businessReader.readLine()
    //    }
    //    businessReader.close()
    //    println("businessList" + businessMap.size)
    //    val businessBroad: Broadcast[mutable.HashMap[String, List[String]]] = sc.broadcast(businessMap)

    //(imsi,starttime,endtime,staytime,longitude,latitude,code,grid,callno,imei,day_id  partition day_id)
    // 1.2将工参信息广播
    val bd_cellMap: Broadcast[util.HashMap[lang.Long, Cell]] = sc.broadcast(cellMap)

    val imsiAllTrace = sc.textFile(inputPath).filter(x => {
      !x.contains("unknow") && !x.contains("null") && !x.contains("\\N")
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
      }).groupBy(_.date)
      .map(x => {
        val imsi: String = x._1
        (imsi, x._2.toList)
      })

    // 识别出租车、快递人员和公交车司机
    // 重复的扇形小区必须要大于这个阈值，平均重复次数是一个范围，两个值来判断
    // 出租车司机，平均重复次数最小
    // 快递人员，平均重复次数居中
    // 公交车司机，平均重复次数最大
    val imsiCellNum: RDD[(String, Int, Int)] = imsiAllTrace.map(x => {
      var allCell = 0
      var avgSameCell = 0
      var allSameCell = 0

      x._2.foreach(x => {
        val allCellList = new ListBuffer[String]
        x.wheres.foreach(y => {
          allCellList.append(y.code)
        })
        allCell += allCellList.size
        allSameCell = allCellList.size - allCellList.distinct.size
      })

      avgSameCell = allSameCell / x._2.length

      val imsi: String = x._2.head.imsi.split("_")(1)
      val imei: String = x._2.head.imsi.split("_")(2)
      val phone: String = x._2.head.imsi.split("_")(0)

      (imsi+"_"+imei+"_"+phone, allCell, avgSameCell)
    })

    val filterTrace: RDD[(String, Int, Int)] = imsiCellNum.filter(x => {
      x._2 > sameCellThreshold
    })

    // 确定各岗人员，分三部分
    // 重复小区最少是出租车司机
    val uberInfo: RDD[(String, String, String, String, String)] = filterTrace.filter(x => {
      x._3 < avgSameCellTimeMin
    }).map(x => {
      val imsi: String = x._1.split("_")(0)
      val imei: String = x._1.split("_")(1)
      val phone: String = x._1.split("_")(2)
      (imsi, imei, phone, "出租车司机", date)
    })

    // 重复小区中等的快递人员
    val courierInfo: RDD[(String, String, String, String, String)] = filterTrace.filter(x => {
      (x._3 > avgSameCellTimeMin) && (x._3 < avgSameCellTimeMax)
    }).map(x => {
      val imsi: String = x._1.split("_")(0)
      val imei: String = x._1.split("_")(1)
      val phone: String = x._1.split("_")(2)
      (imsi, imei, phone, "快递人员", date)
    })

    // 重复小区最多的公交车司机
    val busDriverInfo: RDD[(String, String, String, String, String)] = filterTrace.filter(x => {
      x._3 > avgSameCellTimeMax
    }).map(x => {
      val imsi: String = x._1.split("_")(0)
      val imei: String = x._1.split("_")(1)
      val phone: String = x._1.split("_")(2)
      (imsi, imei, phone, "公交车司机", date)
    })

    val allInfo: RDD[(String, String, String, String, String)] = uberInfo.union(courierInfo).union(busDriverInfo)

    val path = new Path(outputPath)
    val fs = path.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(path)) {
      fs.delete(path, true)
      System.out.println(outputPath + "已存在，删除")
    }
    // 将这个结果先进行保存
    allInfo.saveAsTextFile(outputPath)

    sc.stop()
    println("success!————人群标签识别模型" + date)
  }
}
























