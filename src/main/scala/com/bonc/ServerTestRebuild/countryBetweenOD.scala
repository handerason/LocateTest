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
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}

/**
 * 国际间用户OD,用满出漫入的数据做，国际间的用户OD需要将融合轨迹的数据和漫入漫出表进行整合
 *
 * @author opslos
 * @date 2021/4/30 16:18:09
 */
object countryBetweenOD {
  def main(args: Array[String]): Unit = {

    // 读取用户的融合定位和话单融合的轨迹表，从轨迹表中获取用户的国际OD数据
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

//    val str_hour = args.length match {
//      case 1 => null
//      case 2 => args(1)
//      case _ => null
//    }

    val conf = new SparkConf().setAppName("countryBetweenOD")
    ReadParam.readXML(conf, configPath)

    val isLocal = conf.get("local")
    if ("1".equals(isLocal)) {
      conf.setMaster("local[*]")
    }

    val sixteenTF = new SimpleDateFormat("yyyyMMdd HH:mm:ss")

    // 国信漫出用户详单表
    val inputDateBase: String = conf.get("inputDataBase")
    val inputTable: String = conf.get("inputTable")

    // 结果表
    val outputDateBase: String = conf.get("outputDataBase")
    val outputTable: String = conf.get("outputTable")

    val sc = new SparkContext(conf) // 创建SparkContext
    val hiveContext = new HiveContext(sc)

    //读取hdfs：.dat
    val dataTextFile: BufferedReader = FileUtil.getBufferedReader(new Path(conf.get("datPath")+str_date+"_00_001.dat"))
    var line: String = ""
    line = dataTextFile.readLine()
    val list = new mutable.HashMap[String, String]()
    while (line != null) {
      //（电话号码，日数据分区+开始时间+结束时间+国家编码+国家名称+国家英文名称）
      list.put("86" + line.split("\\|", -1)(1), line.split("\\|", -1)(0) + "\t" + line.split("\\|", -1)(3) + "\t" + line.split("\\|", -1)(4) + "\t" + line.split("\\|", -1)(5) + "\t" + line.split("\\|", -1)(6) + "\t" + line.split("\\|", -1)(7))
      line = dataTextFile.readLine()
    }
    dataTextFile.close()
    println("list" + list.size)
    val CountryODBroad: Broadcast[mutable.HashMap[String, String]] = sc.broadcast(list)

    // 读取国信漫出用户详单表（表1）数据
    //    （imsi，电话号码，基站经度，基站纬度，eci，stat_hour）
    val readSQL = s"select imsi,msisdn,start_longitude,start_latitude,ci,stat_hour from ${inputDateBase}.${inputTable} where stat_hour like concat('${str_date}','%')"
    val readInRDD: RDD[(String, String, String, String, String)] = hiveContext.sql(readSQL).rdd.map(x => {
      var imsi: String = ""
      if (!x.isNullAt(0) && !x.get(0).toString.isEmpty) {
        imsi = x.get(0).toString
      }
      var msisdn: String = ""
      if (!x.isNullAt(1) && !x.get(1).toString.isEmpty) {
        msisdn = x.get(1).toString
      }
      var start_longitude: String = ""
      if (!x.isNullAt(2) && !x.get(2).toString.isEmpty) {
        start_longitude = x.get(2).toString
      }
      var start_latitude: String = ""
      if (!x.isNullAt(3) && !x.get(3).toString.isEmpty) {
        start_latitude = x.get(3).toString
      }
      var ci: String = ""
      if (!x.isNullAt(4) && !x.get(4).toString.isEmpty) {
        ci = x.get(4).toString
      }
      (imsi, msisdn, start_longitude, start_latitude, ci)
    })
    println("readInRDD:" + readInRDD.count())


    //（电话号码，日数据分区+开始时间+结束时间+国家编码+国家名称+国家英文名称）
    //    （imsi，电话号码，基站经度，基站纬度，eci，stat_hour）
    val whereRDD: RDD[(String, Where)] = readInRDD.map(x => {
      val countryODBroad: mutable.HashMap[String, String] = CountryODBroad.value
      val where = new Where()
      if (countryODBroad.contains(x._2)) {
        where.code = x._5
      }
      else {
        where.code = ""
      }
      if (x._3.nonEmpty && x._4.nonEmpty) {
        where.lLA = new LLA(NumberUtils.toDouble(x._3), NumberUtils.toDouble(x._4))
      } else {
        where.lLA = new LLA(0D, 0D)
      }
      //国家映射*********
      //国家编码+国家名称+国家英文名称
      if (countryODBroad.contains(x._2)) {
        where.provinceCity = countryODBroad(x._2).split("\t", -1)(3) + "\u0001" + countryODBroad(x._2).split("\t", -1)(4) + "\u0001" + countryODBroad(x._2).split("\t", -1)(5)
      }
      //开始时间和结束时间映射hfds上的那个文件中的开始时间和结束时间
      if (countryODBroad.contains(x._2)) {
        where.sTime = NumberUtils.toLong(countryODBroad(x._2).split("\t", -1)(1))
      }
      if (countryODBroad.contains(x._2)) {
        where.eTime = NumberUtils.toLong(countryODBroad(x._2).split("\t", -1)(2))
      }
      (x._1 + "_" + x._2, where)
    })
    println("whereRDD:" + whereRDD.count())

    val traceRDD: RDD[Trace] = whereRDD
      .groupByKey()
      .map(x => {
        val sortWheres: List[Where] = x._2.toList.sortBy(_.sTime)
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
      x._2.foreach(y => {
        trace.loadAsLast(y)
      })
      trace.imsi = x._1.split("_",-1)(0)
      trace.pohoneNum = x._1.split("_",-1)(1)
      trace.date = str_date
      trace
    })
    println("traceRDD:" + traceRDD.count())
    //    日期#
    //    IMSI#
    //    手机号码#
    //    省O点基站小区标识
    //    O点基站归属省编码#
    //    O点基站归属省名称#
    //    O点基站所属场景
    //    到达省O点基站时间#
    //    省O点出发时间#
    //    D点基站编号
    //    D点基站归属省编码#
    //    D点基站归属省名称#
    //    D点基站所属场景
    //    D点基站到达时间#
    //    D点省最后离开基站时间#
    //    省间OD距离（公里）
    //    省间OD时长（小时）
    //    省间OD速度
    //    省间OD出行方式
    //    手机号码归属省#
    val resultRDD: RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)] = traceRDD.flatMap(x => {
      val all20list = new ListBuffer[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)]
      var thisProvince = ""
      var lastProvince = ""
      //国家编码+国家名称+国家英文名称
      for (elem <- x.wheres) {
        breakable {
          thisProvince = elem.provinceCity.split("\u0001", -1)(0)
          if (thisProvince.isEmpty && lastProvince.isEmpty) {
            thisProvince = elem.provinceCity.split("\u0001", -1)(0)
            lastProvince = elem.provinceCity.split("\u0001", -1)(0)
            break()
          }
          if (!thisProvince.equals(lastProvince)) {
            val thisWhere: Where = elem
            var lastWhere: Where = elem
            if (x.wheres.indexOf(elem) != x.wheres.length - 1 && x.wheres.indexOf(elem) != 0 && x.wheres.indexOf(elem) != 1) {
              lastWhere = x.wheres(x.wheres.indexOf(elem) - 1)
            }
            // 进行处理
            // 日期,IMSI,手机号码,地市O 点基站小区标识,O 点基站归属地市编码,O 点基站归属地市名称,O 点基站所属场景,到达地市O点基站时间,地市O 点出发时间,D 点基站编号,D点基站归属地市编码,D点基站归属地市名称,D 点基站所属场景,D 点基站到达时间,D点地市最后离开基站时间,地市间OD距离（公里）,地市间OD时长（小时）,地市间OD速度,地市间OD出行方式,手机号码归属省,
            //    O点基站归属省编码#
            //    O点基站归属省名称#
            //    O点基站所属场景
            var ocountryName: String = ""
            if (lastWhere.provinceCity.split("\u0001", -1).isDefinedAt(1)) {
              ocountryName = lastWhere.provinceCity.split("\u0001", -1)(1) + "_" + lastWhere.provinceCity.split("\u0001", -1)(2)
            }
            var ocountryNum: String = ""
            if (lastWhere.provinceCity.split("\u0001", -1).isDefinedAt(0)) {
              ocountryNum = lastWhere.provinceCity.split("\u0001", -1)(0)
            }
            val ocountryScene: String = ""

            //    D点基站归属省编码#
            //    D点基站归属省名称#
            //    D点基站所属场景
            var dcountryName: String = ""
            if (thisWhere.provinceCity.split("\u0001", -1).isDefinedAt(1)) {
              dcountryName = thisWhere.provinceCity.split("\u0001", -1)(1) + "_" + thisWhere.provinceCity.split("\u0001", -1)(2)
            }
            var dcountryNum: String = ""
            if (thisWhere.provinceCity.split("\u0001", -1).isDefinedAt(0)) {
              dcountryNum = thisWhere.provinceCity.split("\u0001", -1)(0)
            }
            val dcountryScene: String = ""


            val costSeconds: Double = (thisWhere.sTime - lastWhere.eTime) / 1000

            all20list.append((x.date, x.imsi, "", lastWhere.code, ocountryNum, ocountryName, ocountryScene, lastWhere.sTime.toString, lastWhere.eTime.toString, thisWhere.code, dcountryNum, dcountryName, dcountryScene, thisWhere.sTime.toString, thisWhere.eTime.toString, "", ""))

            lastProvince = thisProvince
          }
        }

      }
      all20list
    })
    println("resultRDD:" + resultRDD.count())

    val rowRDD: RDD[Row] = resultRDD.map(x => Row(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9, x._10, x._11, x._12, x._13, x._14, x._15, x._16, x._17))

    val schema = "date_day,imsi,msisdn,O_cellID,O_country_code,O_country_name,O_cell_state,O_in_time,O_out_time,D_cellID,D_country_code,D_country_name,D_cell_state,D_in_time,D_out_time,OD_country_type,home_province"
    val structType: StructType = StructType(schema.split(",").map(x => StructField(x, StringType, true)))
    val dataFrame: DataFrame = hiveContext.createDataFrame(rowRDD, structType)
    dataFrame.createOrReplaceTempView("tmpTable")
    val insertSQL: String = s"insert overwrite table ${outputDateBase}.${outputTable} partition(prodate='${str_date}',stat_hour='${str_date}00') " +
      "select date_day,imsi,msisdn,O_cellID,O_country_code,O_country_name,O_cell_state,O_in_time,O_out_time,D_cellID,D_country_code,D_country_name,D_cell_state,D_in_time,D_out_time,OD_country_type,home_province from tmpTable"
    println(insertSQL)
    hiveContext.sql(insertSQL)


    sc.stop()
    println("success!————国际OD完成" + str_date)

  }
}


























