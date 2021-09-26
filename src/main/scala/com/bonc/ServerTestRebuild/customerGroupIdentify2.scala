package com.bonc.ServerTestRebuild

import java.io.BufferedReader
import java.text.SimpleDateFormat
import java.time.LocalDate
import java.util.{Calendar, Date}

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

import scala.util.control.Breaks.{break, breakable}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * @Author: whzHander 
 * @Date: 2021/5/26 10:35 
 * @Description:基于位置的客群识别模型2（教务工作者、学生、医务工作者、商务人员）
 */
object customerGroupIdentify2 {
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
    val conf = new SparkConf().setAppName("customerGroupIdentify2")
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
    val dataTextFile: BufferedReader = FileUtil.getBufferedReader(new Path(conf.get("Path")))
    val List = new mutable.HashMap[String, String]()
    var line: String = ""
    line = dataTextFile.readLine()
    while (line != null) {
      if (!line.contains("-")) {
        // eci，场景id+场景名称
        if (line.split("\\|").length >= 5) {
          List.put(line.split("\\|")(2), line.split("\\|")(0) + "_" + line.split("\\|")(1))
        }
      }
      line = dataTextFile.readLine()
    }
    dataTextFile.close()
    println("List" + List.size)
    val Broad: Broadcast[mutable.HashMap[String, String]] = sparkContext.broadcast(List)

    //读取hdfs：CUST_20210524.txt

    val dataTextFile2: BufferedReader = FileUtil.getBufferedReader(new Path(conf.get("Path2")))
    val List2 = new mutable.HashMap[String, String]()
    var line2: String = ""
    line2 = dataTextFile2.readLine()
    var i: Int = 0
    while (line2 != null && i < 100) {
      //      （IMSI，CALLING_NO+AGE+BILL_INCOME+homeareacode+workareacode）
      List2.put(line2.split("\\|", -1)(2), line2.split("\\|", -1)(1) + "_" + line2.split("\\|", -1)(3) + "_" + line2.split("\\|", -1)(4) + "_" + line2.split("\\|", -1)(5) + "_" + line2.split("\\|", -1)(7))
      line2 = dataTextFile2.readLine()
      i += 1
    }
    dataTextFile2.close()
    println("List2" + List2.size)
    val Broad2: Broadcast[mutable.HashMap[String, String]] = sparkContext.broadcast(List2)

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

    val whereRDD: RDD[(String, Where, String, String, String)] = readInRDD.map(x => {
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


    val traceRDD: RDD[(String, String, Trace)] = wheresRDD.map((x: (String, ListBuffer[Where], String, String, String)) => {
      val trace = new Trace(LocType.SECTOR)
      x._2.foreach(y => {
        trace.loadAsLast(y)
      })
      trace.imsi = x._1
      trace.pohoneNum = ""
      trace.date = x._5
      //      （callno,imei,trace）
      (x._3, x._4, trace)
    })
    println("traceRDD" + traceRDD.count())

    val anotherRDD: RDD[(String, String, Int, Double, String, String, String, Trace)] = traceRDD.map(x => {
      val broad2: mutable.HashMap[String, String] = Broad2.value

      var workArea: String = ""
      var homeArea: String = ""
      var age: Int = 0
      var income: Double = 0
      var calling_no: String = ""
      val date: String = x._3.date
      val imei: String = x._2


      if (broad2.contains(x._3.imsi) && !broad2(x._3.imsi).split("_")(0).isEmpty && !broad2(x._3.imsi).split("_")(1).isEmpty && !broad2(x._3.imsi).split("_")(2).isEmpty && !broad2(x._3.imsi).split("_")(3).isEmpty && !broad2(x._3.imsi).split("_")(4).isEmpty) {
        //        date = broad2(x._3.imsi).split("_", -1)(0)

        calling_no = broad2(x._3.imsi).split("_")(0)
        age = broad2(x._3.imsi).split("_")(1).toInt
        income = broad2(x._3.imsi).split("_")(2).toDouble
        homeArea = broad2(x._3.imsi).split("_")(3)
        workArea = broad2(x._3.imsi).split("_")(4)
      }

      (date, calling_no, age, income, homeArea, workArea, imei, x._3)
    })
    println("anotherRDD:" + anotherRDD.count())

    var weekendOnWork: Int = 0
    var weekendOffWork: Int = 0
    val teacherRDD: RDD[(String, String, String, String, String)] = anotherRDD.filter(x => {
      val broad: mutable.HashMap[String, String] = Broad.value
      val dateFormat: Long = new SimpleDateFormat("yyyyMMdd").parse(x._8.date).getTime
      val secondTime = tranTimeToString(dateFormat.toString)
      val week = LocalDate.parse(secondTime).getDayOfWeek

      var bool: Boolean = false
      //      教务工作者
      //      工作地是学校
      if (broad.contains(x._6) && broad.contains(x._5)) {
        if (broad(x._6).split("_", -1)(1).equals("学校") && x._3 > 23) {
          if (!broad(x._5).split("_", -1)(1).equals(broad(x._6).split("_", -1)(1))) {
            if (week.toString.equals("SATURDAY") || week.toString.equals("SUNDAY")) {
              for (elem <- x._8.wheres) {
                breakable {
                  if (elem.code.equals(x._6)) {
                    weekendOnWork += 1
                    bool = true
                    break()
                  }
                  else {
                    weekendOffWork += 1
                    bool = true
                    break()
                  }
                }
              }
            }
          }
          else if (x._4 > 100) {
            bool = true
          }
        }
      }
      bool
    }).flatMap(x => {
      val all5list = new ListBuffer[(String, String, String, String, String)]
      if ((weekendOffWork / weekendOnWork) > 1) {
        all5list.append((x._8.imsi, x._2, x._7, "教务人员", x._1))
      }
      all5list
    })
    println("teacherRDD:" + teacherRDD.count())

    val studentRDD: RDD[(String, String, String, String, String)] = anotherRDD.flatMap(x => {
      val broad: mutable.HashMap[String, String] = Broad.value
      val all5list = new ListBuffer[(String, String, String, String, String)]
      if (broad.contains(x._6)) {
        if (broad(x._6).split("_", -1)(1).equals("学校") && x._3 > 23) {
          all5list.append((x._8.imsi, x._2, x._7, "学生", x._1))
        }
      }
      all5list
    })
    println("studentRDD:" + studentRDD.count())

    val doctorRDD: RDD[(String, String, String, String, String)] = anotherRDD.filter(x => {
      val broad: mutable.HashMap[String, String] = Broad.value
      val dateFormat: Long = new SimpleDateFormat("yyyyMMdd").parse(x._8.date).getTime
      val secondTime = tranTimeToString(dateFormat.toString)
      val week = LocalDate.parse(secondTime).getDayOfWeek

      var bool: Boolean = false
      //      医务工作者
      if (broad.contains(x._6) && broad.contains(x._5)) {
        if (broad(x._6).split("_", -1)(1).equals("医院") && x._3 > 23) {
          if (!broad(x._5).split("_", -1)(1).equals(broad(x._6).split("_", -1)(1))) {
            if (week.toString.equals("SATURDAY") || week.toString.equals("SUNDAY")) {
              for (elem <- x._8.wheres) {
                breakable {
                  if (elem.code.equals(x._6)) {
                    weekendOnWork += 1
                    bool = true
                    break()
                  }
                  else {
                    weekendOffWork += 1
                    bool = true
                    break()
                  }
                }
              }
            }
          }
        }
      }
      bool
    }).flatMap(x => {
      val all5list = new ListBuffer[(String, String, String, String, String)]
      if ((weekendOffWork / weekendOnWork) > 1) {
        all5list.append((x._8.imsi, x._2, x._7, "医务工作者", x._1))
      }
      all5list
    })
    println("doctorRDD:" + doctorRDD.count())

    //      (date, calling_no, age, income, homeArea, workArea, imei, x._3)
    val workerRDD: RDD[(String, String, String, String, String)] = anotherRDD.filter(x => {
      val broad: mutable.HashMap[String, String] = Broad.value
      val dateFormat: String = new SimpleDateFormat("yyyy-MM-dd").parse(x._8.date).toString
      val week = LocalDate.parse(dateFormat).getDayOfWeek

      var bool: Boolean = false
      //      商务人员
      if (broad.contains(x._6) && broad.contains(x._5)) {
        if (x._3 >= 18 && x._3 <= 70) {
          if (!broad(x._5).split("_", -1)(1).equals(broad(x._6).split("_", -1)(1))) {
            for (elem <- x._8.wheres) {
              if (elem.code.equals(x._6)) {
                if (tranfTime(elem.sTime.toString) >= 8 && tranfTime(elem.eTime.toString) <= 20) {
                  weekendOnWork += 1
                }
                else {
                  weekendOffWork += 1
                }
              }
            }
          }
        }
      }
      bool
    }).flatMap(x => {
      val all5list = new ListBuffer[(String, String, String, String, String)]
      if ((weekendOffWork / weekendOnWork) < 1) {
        all5list.append((x._8.imsi, x._2, x._7, "商务人员", x._1))
      }
      all5list
    })
    println("workerRDD:" + workerRDD.count())

    val finalRDD: RDD[(String, String, String, String, String)] = teacherRDD.union(studentRDD).union(doctorRDD).union(workerRDD)
    val rowRDD: RDD[Row] = finalRDD.map(x => Row(x._1, x._2, x._3, x._4, x._5))
    val schema = "imsi,msisdn,imei,crowd_name,stat_date"
    val structType: StructType = StructType(schema.split(",").map(x => StructField(x, StringType, true)))
    val dataFrame: DataFrame = hiveContext.createDataFrame(rowRDD, structType)
    dataFrame.createOrReplaceTempView("tmpTable")
    val insertSQL: String = s"insert overwrite table ${outputDataBase}.${outputTable} partition(prodate='${str_date}') " +
      "select imsi,msisdn,imei,crowd_name,stat_date from tmpTable"
    println(insertSQL)
    hiveContext.sql(insertSQL)


    sparkContext.stop()
    println("success!————客群识别2完成" + str_date)
  }

  def tranfTime(timestring: String): Int = {
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //将string的时间转换为date
    val time: Date = fm.parse(timestring)

    val cal = Calendar.getInstance()
    cal.setTime(time)
    //提取时间里面的小时时段
    val hour: Int = cal.get(Calendar.HOUR_OF_DAY)
    hour
  }

  def tranTimeToString(tm: String): String = {
    val fm = new SimpleDateFormat("yyyy-MM-dd")
    val tim = fm.format(new Date(tm.toLong))
    tim
  }
}
