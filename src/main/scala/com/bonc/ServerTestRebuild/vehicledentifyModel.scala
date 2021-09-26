package com.bonc.ServerTestRebuild

import java.io.BufferedReader
import java.text.SimpleDateFormat

import com.bonc.data.Record
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
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}
import scala.collection._
object vehicledentifyModel {
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
    conf.setAppName("vehicleModel" + date)
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
    val hiveContext = new HiveContext(sc)


    val inputDataBase: String = conf.get("inputDataBase")
    val inputTale: String = conf.get("inputTale")
    val outputDataBase: String = conf.get("outputDataBase")
    val outputTable: String = conf.get("outputTable")

    //时间处理类
    val dateFAllLinks = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    //读取基站类型
    val gcReader: BufferedReader = FileUtil.getBufferedReader(new Path(conf.get("G4Path")))
    val G2List = new mutable.HashMap[String, String]()
    val G4List = new mutable.HashMap[String, String]()
    var gc: String = ""
    gc = gcReader.readLine()
    println(gc)
    while (gc != null) {
      if (!gc.contains("-")) {
        // (ECI,基站类型)
        if (gc.split("\\|").length >= 5) {
          if (gc.split("\\|")(2).contains("_")) {
            if(gc.split("\\|")(1).equals("飞机场")||gc.split("\\|")(1).equals("高铁站")) {
              G2List.put(gc.split("\\|")(2), gc.split("\\|")(1))
            }
          } else {
            if(gc.split("\\|")(1).equals("飞机场")||gc.split("\\|")(1).equals("高铁站")) {
              G4List.put(gc.split("\\|")(2), gc.split("\\|")(1))
            }
          }
        }
      }
      gc = gcReader.readLine()
    }
    gcReader.close()
    println("G4List" + G4List.size)
    println("G2List" + G2List.size)
    val G4Broad: Broadcast[mutable.HashMap[String, String]] = sc.broadcast(G4List)
    val G2Broad: Broadcast[mutable.HashMap[String, String]] = sc.broadcast(G2List)

    //读取离线位置模型
    val readSQL = s"select imsi,starttime,endtime,staytime,longitude,latitude,code,callno,imei,day_id from ${inputDataBase}.${inputTale} where day_id='${date}'"
    val inputRDD: RDD[(String,String,String,String,String,String,String,String,String,String)] =hiveContext.sql(readSQL)
      .rdd
      .map(x=>{
        val imsi = x.get(0).toString
        val starttime = x.get(1).toString
        val endtime = x.get(2).toString
        val staytime = x.get(3).toString
        val longitude = x.get(4).toString
        val latitude = x.get(5).toString
        val code = x.get(6).toString
        val callno = x.get(7).toString
        val imei = x.get(8).toString
        val day_id = x.get(9).toString
        (imsi,starttime,endtime,staytime,longitude,latitude,code,callno,imei,day_id)
      })
    println("inputRDD"+inputRDD.count())
    inputRDD.take(5).foreach{println}


    val traceRDD=inputRDD.map(x=>{
      val where = new Where
      //imsi
      val imsi=x._1
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
      where.sTime = dateFAllLinks.parse(x._2).getTime
      // eTime
      where.eTime = dateFAllLinks.parse(x._3).getTime
      // phone
      val phone: String = x._8
      // imei
      val imei: String = x._9
      (imsi+"\t"+phone+"\t"+imei, where)
    })
    println("traceRDD"+traceRDD.count())
    traceRDD.take(5).foreach{println}
//    //平滑处理
//    val anotherRDD = traceRDD.map(x=>{
//      val sortWheres = new ListBuffer[Where]()
//      sortWheres.append(x._2)
//      for(elem<-sortWheres) {
//        if (sortWheres.indexOf(elem) != sortWheres.length - 1){
//          if (elem.eTime > sortWheres(sortWheres.indexOf(elem) + 1).sTime) {
//            elem.eTime = Math.min(elem.eTime, sortWheres(sortWheres.indexOf(elem) + 1).sTime)
//            sortWheres(sortWheres.indexOf(elem) + 1).sTime = Math.min(elem.eTime, sortWheres(sortWheres.indexOf(elem) + 1).sTime)
//          }
//        }
//      }
//      (x._1,sortWheres)
//    })
//    println("anotherRDD"+anotherRDD.count())
//    anotherRDD.take(5).foreach{println}
    val anotherRDD = traceRDD.groupByKey()
      .map(x => {
        val recordList = x._2.toList
        val sortRecordList = recordList.sortBy(_.sTime).sortBy(_.eTime)
        val trace = new Trace(LocType.SECTOR)
        trace.imsi = x._1.split("\t", -1)(0)
        trace.pohoneNum = x._1.split("\t", -1)(1) + "\t" +
          x._1.split("\t", -1)(2)
        trace.createByWhere(sortRecordList)
        trace
      })
    println("anotherRDD"+traceRDD.count())
//    val valueRDD = anotherRDD
//      .map(x=>{
//      val trace = new Trace(LocType.SECTOR)
//      val recordList = x._2.toList
//      val sortRecordList = recordList.sortBy(_.sTime).sortBy(_.eTime)
////      x._2.foreach(y=>{
////        trace.loadAsLast(y)
////      })
////      //imsi+imei
////      val imsi = x._1+"\t"+x._4
////      trace.imsi = x._1
////      trace.pohoneNum = x._3
////      (imsi,trace)
//      trace
//    })
//    println("valueRDD"+valueRDD.count())

    val mergeWhereTrace = anotherRDD.map(x=>{
      val g2Broad: mutable.HashMap[String, String] = G2Broad.value
      val g4Broad: mutable.HashMap[String, String] = G4Broad.value
      x.wheres.foreach(y=>{
        if(y.rat == 2&&g2Broad.contains(y.code)){
          y.source = g2Broad(y.code)
        }else if (y.rat == 6 && g4Broad.contains(y.code)) {
          y.source = g4Broad(y.code)
        }
      })
      // 上边识别完成之后进行合并，将交通枢纽的数据进行合并
//      val whereList: List[Where] = x.wheres.toList.sortBy(_.sTime)
//      //      val wheres = new ListBuffer[Where]
//      //      val tmpWheres = new ListBuffer[Where]
//      //      var choseEnbid = ""
//      //      for (elem <- whereList) {
//      //        breakable {
//      //          // index为0，一开始
//      //          if (whereList.indexOf(elem) == 0) {
//      //            choseEnbid = elem.code
//      //            tmpWheres.append(elem)
//      //            break()
//      //          } else {
//      //            // 遍历其后元素
//      //            if (choseEnbid == elem.code) {
//      //              tmpWheres.append(elem)
//      //              break()
//      //            } else if (choseEnbid != elem.code || whereList.indexOf(elem) == whereList.length - 1) {
//      //              val where = new Where()
//      //              val sortTmpWhere: ListBuffer[Where] = tmpWheres.sortBy(_.sTime)
//      //              // 属性赋值
//      //              where.sTime = sortTmpWhere.head.sTime
//      //              where.eTime = sortTmpWhere.last.eTime
//      //              where.code = choseEnbid
//      //
//      //              wheres.append(where)
//      //              tmpWheres.clear()
//      //              choseEnbid = elem.code
//      //            }
//      //          }
//      //        }
//      //      }
//      val trace = new Trace(x.imsi, x.traceType)
//      trace.date = x.date
//      whereList.foreach(y => {
//        trace.loadAsLast(y)
//      })
      x
//    }).filter(x=>{
//      // 过滤掉没有出行信息的Trace
//      val G4Map: mutable.HashMap[String, String] = G4Broad.value
//      val places: List[String] = G4Map.values.toList
//      val wheres = new ListBuffer[Where]
//      x._2.wheres.foreach(y => {
//        if (places.contains(y.code)) {
//          wheres.append(y)
//        }
//      })
//      wheres.distinct.length >= 2
    }).flatMap(x=> {
      val all9list = new ListBuffer[(String, String, String, String, String, String, String, String, String)]
      var endHub: String = ""
      var startHub: String = ""
      var timeStamp = 0L
      var duration = 0D
      var mileage = 0D
      var tsType = ""
      var startType = ""
      var endType = ""
      for (elem: Where <- x.wheres) {
        breakable {
          startType = elem.source
          if (startType.isEmpty && endType.isEmpty) {
            startType = elem.source
            endType = elem.source
            break()
          }
          if (!startType.equals(endType)) {
            var startWhere: Where = elem
            if (x.wheres.indexOf(elem) != x.wheres.length - 1 && x.wheres.indexOf(elem) != 0 && x.wheres.indexOf(elem) != 1) {
              val endWhere = x.wheres(x.wheres.indexOf(elem) - 1)
              endHub = endWhere.code
              duration = endWhere.eTime - startWhere.sTime
              mileage = startWhere.getDistance(endWhere)
              endType = endWhere.source
            }
            timeStamp = startWhere.sTime
            var imei = x.pohoneNum.split("\t",-1)(0)
            var phone = x.pohoneNum.split("\t",-1)(1)
            startHub = startWhere.code
            startType = startWhere.source
            tsType = startType
            // 获得出行方式
//            if(startType.equals(endType)&&startType.equals("飞机场")){
//              tsType = "飞机"
//            }else if(startType.equals(endType)&&startType.equals("高铁站")){
//              tsType = "高铁"
//            }
            all9list.append((timeStamp.toString, x.imsi, phone, imei, startHub, endHub, tsType, mileage.toString, duration.toString))
            endType = startType
          }
        }
        //      // 按照时间排序，获得出发点和终点
        //      val chosenWhere: ListBuffer[Where] = resultWhere.sortBy(_.sTime)
        //      if (chosenWhere.nonEmpty){
        //        // 出发点
        //        val startWhere: Where = chosenWhere.head
        //
        //        startHub = startWhere.code
        //        startType = startWhere.source
        //        // 终点
        //        val arriverWheres: ListBuffer[Where] = chosenWhere.filter(!_.code.equals(startWhere.code))
        //        if (arriverWheres.nonEmpty){
        //          val arriveWhere: Where = arriverWheres.minBy(_.sTime)
        //          endHub = arriveWhere.code
        //          endType = arriveWhere.source
        //          // 时间间隔和距离
        //          duration = arriveWhere.sTime - startWhere.eTime
        //          mileage = startWhere.getDistance(arriveWhere)
        //        }
      }
      all9list
    })
//      .filter(x=>{
//      !x._7.isEmpty
//    })
    println("mergeWhereTrace"+mergeWhereTrace.count())

    val rowRDD: RDD[Row] = mergeWhereTrace.map(x => Row(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9))
    println(rowRDD.count())
    val schema = "timeStamp,imsi,phone,imei,start_transportation,end_transportation,trans_type,mileage,duration"
    val structType: StructType = StructType(schema.split(",").map(x => StructField(x, StringType, true)))
    val dataFrame: DataFrame = hiveContext.createDataFrame(rowRDD, structType)
    dataFrame.createOrReplaceTempView("tmpTable")
    val insertSQL: String = s"insert overwrite table ${outputDataBase}.${outputTable} partition(day_id='" + date + "') " +
      "select timeStamp,imsi,phone,imei,start_transportation,end_transportation,trans_type,mileage,duration from tmpTable"
    println(insertSQL)
    hiveContext.sql(insertSQL)


    sc.stop()
    println("success!————交通工具识别模型" + date)

  }
}
