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
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * 行政区客群流入流出
 *
 * @author opslos
 * @date 2021/4/27 16:20:05
 */
object administrativeRegionFlow {
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

    val conf = new SparkConf().setAppName("mergeTraceCellFlow")
    ReadParam.readXML(conf, configPath)

    val isLocal = conf.get("local")
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
    val inputRDD: RDD[(String, String, String, String, String, String, String,String)] = hiveContext.sql(inSQL).rdd.map(x => {
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
      (imsi, lac_cell, rat, longitude, latitude, enter_time, leave_time,county_code)
    })
    println("inputRDD"+inputRDD.count())


    //读取hdfs：YDY_XZQ_ECI_DATA.txt
    val dataTextFile: BufferedReader = FileUtil.getBufferedReader(new Path(conf.get("G4Path")))
    var line: String = ""
    line=dataTextFile.readLine()
    val list = new mutable.HashMap[String, String]()
    while (line != null) {
      if (!line.contains("-")) {3
        if (line.split("\\|").length >= 5) {
//          eci，场景id 场景名称 地市id 地市名称 区县id 区县名称
          list.put(line.split("\\|")(2),line.split("\\|")(0)+"\t"+line.split("\\|")(1)+"\t"+line.split("\\|")(3)+"\t"+line.split("\\|")(4)+"\t"+line.split("\\|")(5)+"\t"+line.split("\\|")(6))
        }
      }
      line=dataTextFile.readLine()
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
    val traceRDD: RDD[Trace] = inputRDD.map(x => {
      val where = new Where()
      if(!x._6.equals(0)){
        where.sTime = NumberUtils.toLong(x._6)
      }
      if(!x._6.equals(0)){
        where.eTime = NumberUtils.toLong(x._7)
      }
      where.code = x._2
      where.lLA = new LLA(NumberUtils.toDouble(x._4), NumberUtils.toDouble(x._5))
      where.rat = NumberUtils.toInt(x._3)
      (x._1, where)
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
      .map((x: (String, List[Where])) => {
        val trace = new Trace(LocType.SECTOR)
        x._2.foreach(y => {
          trace.loadAsLast(y)
        })
        trace.imsi = x._1
        trace.date = str_date
        trace
      })
    println("traceRDD"+traceRDD.count())

    //          eci，场景id 场景名称 地市id 地市名称 区县id 区县名称
    val adminTraceRDD: RDD[Trace] = traceRDD.map(x => {
      val ecis: mutable.HashMap[String, String] = eciAdminBroad.value
      x.wheres.foreach(y => {
        val countyId: String = ecis.getOrElse(y.code, "")
        if (countyId.nonEmpty && countyId.split("\t", -1).isDefinedAt(4)) {
          y.provinceCity = ecis.getOrElse(y.code, "").split("\t",-1)(4)
        }
      })
      // 进行where的合并
      val whereList: List[Where] = x.wheres.toList.sortBy(_.sTime)
      val trace = new Trace(x.imsi, x.traceType)
      whereList.foreach(y => {
        trace.loadAsLast(y)
      })
      trace
    })
    println("adminTraceRDD"+adminTraceRDD.count())


    val eciTraceRDD: RDD[(String, String, String, String, String,String)] = adminTraceRDD.flatMap(x => {
      // 时间段，区县id，保持人的imsi，流入人的imsi，流出人的imsi，区域坐标串
      val resultList = new ListBuffer[(String, String, String, String, String,String)]

      val hourFormat = new SimpleDateFormat("yyyyMMdd HH:00:00")
      hourFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))

      x.wheres.toList.sortBy(_.sTime).foreach(y => {
        val sHourStamp: String = hourFormat.format(new Date(y.sTime))
        val eHourStamp: String = hourFormat.format(new Date(y.eTime))
        val sHour: Int = NumberUtils.toInt(sHourStamp.substring(9, 11))
        val eHour: Int = NumberUtils.toInt(eHourStamp.substring(9, 11))
        if (sHourStamp.equals(eHourStamp)) {
          resultList.append((sHourStamp, y.provinceCity, "", x.imsi, "",y.lLA.toString))
          resultList.append((eHourStamp, y.provinceCity, "", "", x.imsi,y.lLA.toString))
        } else if (sHour < eHour) {
          val diff: Int = eHour - sHour
          // 驻留时间
          for (i <- Range(1, diff)) {
            val calendar: Calendar = Calendar.getInstance()
            calendar.setTime(new Date(y.sTime))
            calendar.add(Calendar.HOUR, i)
            val upTime: Date = calendar.getTime
            val upHour: String = hourFormat.format(upTime)
            resultList.append((upHour, y.provinceCity, x.imsi, "", "",y.lLA.toString))
          }
          // 进入
          resultList.append((sHourStamp, y.provinceCity, "", x.imsi, "",y.lLA.toString))
          // 离开
          resultList.append((eHourStamp, y.provinceCity, "", "", x.imsi,y.lLA.toString))
        }
      })
      resultList
    })
    println("eciTraceRDD"+eciTraceRDD.count())

    // 时间段，区县id，保持人的imsi，流入人的imsi，流出人的imsi
    val resultRDD: RDD[(String, String, String, String, String, String, String, String, String)] = eciTraceRDD.map((x: (String, String, String, String, String, String)) => {
      val str: String = x._1 + "\t" + x._2
      (str, x._3, x._4, x._5,x._6)
    }).groupBy((_: (String, String, String, String, String))._1).mapPartitions((x: Iterator[(String, Iterable[(String, String, String, String, String)])]) => {
      val listBuffer = new ListBuffer[(String, String, String, String, String, String, String, String, String)]
      val ecis: mutable.HashMap[String, String] = eciAdminBroad.value

      val hashMap = new util.HashMap[String, String]()
      //          eci，场景id 场景名称 地市id 地市名称 区县id 区县名称

      ecis.values.toList.distinct.foreach((y: String) => {
        val strings: Array[String] = y.split("\t")
        hashMap.put(strings(4),strings(5)+"\t"+strings(2))
      })
      while(x.hasNext){
        val tuple: (String, Iterable[(String, String, String, String,String)]) = x.next()
        val strings: Array[String] = tuple._1.split("\t",-1)
        val hourStamp: String = strings(0)
        var name = ""
        var llAs = ""
        var code:String=""
        if(strings.isDefinedAt(1)){
          code = strings(1)//区县id
        }
        val nameLLA: String = hashMap.getOrDefault(code, "")
        if (!"".equals(nameLLA)){
          name = nameLLA.split("\t")(0)
          llAs = nameLLA.split("\t")(1)
        }
        val stay = new ListBuffer[String]
        val inFlow = new ListBuffer[String]
        val outFlow = new ListBuffer[String]
        for (elem <- tuple._2) {
          stay.append(elem._2)
          inFlow.append(elem._3)
          outFlow.append(elem._4)
        }
        val stayCount: Int = stay.count(y => y.nonEmpty)
        val inFlowCount: Int = inFlow.count(y => y.nonEmpty)
        val outFlowCount: Int = outFlow.count(y => y.nonEmpty)
        val totalNum: Int = stayCount + inFlowCount - outFlowCount
        listBuffer append ((hourStamp, code, name,llAs, totalNum.toString, stayCount.toString, inFlowCount.toString, outFlowCount.toString, hourStamp.split(" ")(0)))
      }
      listBuffer.toIterator
    })
    println("resultRDD:"+resultRDD.count())

    val rowRDD: RDD[Row] = resultRDD.map(x=>{
      Row(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8)
    })
    val schema = "hourStamp,code,name,llAs,totalNum,stayCount,inFlowCount,outFlowCount"
    val structType: StructType = StructType(schema.split(",").map(x => StructField(x, StringType, true)))
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





















