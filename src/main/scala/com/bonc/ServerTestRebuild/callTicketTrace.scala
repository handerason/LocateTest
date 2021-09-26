package com.bonc.ServerTestRebuild

import java.text.SimpleDateFormat
import java.util.Date

import com.bonc.lla.LLA
import com.bonc.location.LocType
import com.bonc.trace.{Trace, Where}
import com.bonc.user_locate.ReadParam
import org.apache.commons.lang.math.NumberUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable.ListBuffer

/**
 * 话单轨迹生成
 *
 * @author opslos
 * @date 2021/4/28 11:21:02
 */
object callTicketTrace {
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
    val str_hour = args.length match {
      case 1 => null
      case 2 => null
      case 3 => args(2)
      case _ => null
    }

    val conf = new SparkConf().setAppName("callTicketTrace")
    ReadParam.readXML(conf, configPath)

    val isLocal = conf.get("local")
    if ("1".equals(isLocal)) {
      conf.setMaster("local[*]")
    }

    val inputLocalProvinceVoiceDataBase: String = conf.get("inputLocalProvinceVoiceDataBase")
    val inputLocalProvinceVoiceTale: String = conf.get("inputLocalProvinceVoiceTale")
    val inputOutProvinceDataVoiceDataBase: String = conf.get("inputOutProvinceDataVoiceDataBase")
    val inputOutProvinceDataVoiceTable: String = conf.get("inputOutProvinceDataVoiceTable")
    val inputLocalProvinceNetFlowDataBase: String = conf.get("inputLocalProvinceNetFlowDataBase")
    val inputLocalProvinceNetFlowTale: String = conf.get("inputLocalProvinceNetFlowTale")
    val inputOutProvinceDataNetFlowDataBase: String = conf.get("inputOutProvinceDataNetFlowDataBase")
    val inputOutProvinceDataNetFlowTable: String = conf.get("inputOutProvinceDataNetFlowTable")
    val outputDataBse: String = conf.get("outputDataBase")
    val outputTable: String = conf.get("outputTable")

    val sc = new SparkContext(conf) // 创建SparkContext
    val hiveContext = new HiveContext(sc)

    //  时间处理类
    val dateFAllLinks = new SimpleDateFormat("yyyyMMddHHmmss")


    // 读取本省用户的语音和流量话单数据
    // stat_hour,phone_no,lev1_blong_dept,lev2_blong_dept,lev1_roam_dept,lev2_roam_dept,lac_id,cell_id,imsi,start_time,call_dur_s,roam_type,inport_time
    // 账期小时,计费用户号码,一级归属局,二级归属局,一级漫游局,二级漫游局,定位地点识别,基站代码,计费用户标识,通话起始时间,通话时长,漫游类型,入库时间
    val localVoice = s"select phone_no,lev1_roam_dept,lev2_roam_dept,lac_id,cell_id,imsi,start_time,call_dur_s from ${inputLocalProvinceVoiceDataBase}.${inputLocalProvinceVoiceTale} where stat_date=${str_date} and stat_hour=${str_hour}"
    val localNetFlow = s"select phone_no,lev1_roam_dept,lev2_roam_dept,lac_id,cell_id,imsi,start_time,call_dur_s from ${inputLocalProvinceNetFlowDataBase}.${inputLocalProvinceNetFlowTale} where stat_date=${str_date} and stat_hour=${str_hour}"
    val localVoiceRDD: RDD[(String, String, String, String, String, String, String, String)] = hiveContext.sql(localVoice).rdd.map(x => {
      var phone_no: String = ""
      if (!x.isNullAt(0) && !x.get(0).toString.isEmpty) {
        phone_no = x.get(0).toString
      }
      var lev1_roam_dept: String = ""
      if (!x.isNullAt(1) && !x.get(1).toString.isEmpty) {
        lev1_roam_dept = x.get(1).toString
      }
      var lev2_roam_dept: String = ""
      if (!x.isNullAt(2) && !x.get(2).toString.isEmpty) {
        lev2_roam_dept = x.get(2).toString
      }
      var lac_id: String = ""
      if (!x.isNullAt(3) && !x.get(3).toString.isEmpty) {
        lac_id = x.get(3).toString
      }
      var cell_id: String = ""
      if (!x.isNullAt(4) && !x.get(4).toString.isEmpty) {
        cell_id = x.get(4).toString
      }
      var imsi: String = ""
      if (!x.isNullAt(5) && !x.get(5).toString.isEmpty) {
        imsi = x.get(5).toString
      }
      var start_time: String = ""
      if (!x.isNullAt(6) && !x.get(6).toString.isEmpty) {
        start_time = x.get(6).toString
      }
      var call_dur_s: String = ""
      if (!x.isNullAt(7) && !x.get(7).toString.isEmpty) {
        call_dur_s = x.get(7).toString
      }
      (imsi, phone_no, lev1_roam_dept, lev2_roam_dept, lac_id, cell_id, start_time, call_dur_s)
    })
    // 数据处理
    val localVoiceProcessR: RDD[(String, String, String, Long, Long, Long)] = localVoiceRDD map (x => {
      // eci处理   cellID处理：去掉|1100，16-10，如果大于65535，取他自己，如果不是取lac_id(16-10)&"-"&cell_id(去掉|1100,16-10)
      var eci = 0L
      val lac_id: String = x._5
      var cellID: String = x._6
      if (cellID.contains("|")) {
        cellID = cellID.split("\\|")(0)
      }
      var cellIDInt: Int = 0
      if (cellID.nonEmpty) {
        cellIDInt = Integer.parseInt(cellID, 16)
      }
      if (cellIDInt <= 65535 && lac_id.nonEmpty) {
        cellIDInt = Integer.parseInt(lac_id, 16) - cellIDInt
      }
      eci = cellIDInt
      // 时间处理
      val sTime: Long = dateFAllLinks.parse(x._7).getTime
      val eTime: Long = sTime + NumberUtils.toLong(x._8)
      (x._1, x._2, x._3 + "\u0001" + x._4, eci, sTime, eTime)

    })


    val localNetFlowRDD: RDD[(String, String, String, String, String, String, String, String)] = hiveContext.sql(localNetFlow).rdd.map(x => {
      var phone_no: String = ""
      if (!x.isNullAt(0) && !x.get(0).toString.isEmpty) {
        phone_no = x.get(0).toString
      }
      var lev1_roam_dept: String = ""
      if (!x.isNullAt(1) && !x.get(1).toString.isEmpty) {
        lev1_roam_dept = x.get(1).toString
      }
      var lev2_roam_dept: String = ""
      if (!x.isNullAt(2) && !x.get(2).toString.isEmpty) {
        lev2_roam_dept = x.get(2).toString
      }
      var lac_id: String = ""
      if (!x.isNullAt(3) && !x.get(3).toString.isEmpty) {
        lac_id = x.get(3).toString
      }
      var cell_id: String = ""
      if (!x.isNullAt(4) && !x.get(4).toString.isEmpty) {
        cell_id = x.get(4).toString
      }
      var imsi: String = ""
      if (!x.isNullAt(5) && !x.get(5).toString.isEmpty) {
        imsi = x.get(5).toString
      }
      var start_time: String = ""
      if (!x.isNullAt(6) && !x.get(6).toString.isEmpty) {
        start_time = x.get(6).toString
      }
      var call_dur_s: String = ""
      if (!x.isNullAt(7) && !x.get(7).toString.isEmpty) {
        call_dur_s = x.get(7).toString
      }
      (imsi, phone_no, lev1_roam_dept, lev2_roam_dept, lac_id, cell_id, start_time, call_dur_s)
    })
    val localNetFlowProcessR: RDD[(String, String, String, Long, Long, Long)] = localNetFlowRDD.map(x => {
      val sTime: Long = dateFAllLinks.parse(x._7).getTime
      val eTime: Long = sTime + NumberUtils.toLong(x._8)

      (x._1, x._2, x._3 + "\u0001" + x._4, (Integer.parseInt(x._5, 16) * 256).toLong + Integer.parseInt(x._6, 16).toLong, sTime, eTime)
    })

    // 读取外省用户的语音和流量话单数据
    val outVoice = s"select phone_no,lev1_roam_dept,lev2_roam_dept,lac_id,cell_id,imsi,start_time,call_dur_s from ${inputOutProvinceDataVoiceDataBase}.${inputOutProvinceDataVoiceTable} where stat_date=${str_date} and stat_hour=${str_hour}"
    val outNetFlow = s"select phone_no,lev1_roam_dept,lev2_roam_dept,lac_id,cell_id,imsi,start_time,call_dur_s from ${inputOutProvinceDataNetFlowDataBase}.${inputOutProvinceDataNetFlowTable} where stat_date=${str_date} and stat_hour=${str_hour}"
    val outVoiceRDD: RDD[(String, String, String, String, String, String, String, String)] = hiveContext.sql(outVoice).rdd.map(x => {
      var phone_no: String = ""
      if (!x.isNullAt(0) && !x.get(0).toString.isEmpty) {
        phone_no = x.get(0).toString
      }
      var lev1_roam_dept: String = ""
      if (!x.isNullAt(1) && !x.get(1).toString.isEmpty) {
        lev1_roam_dept = x.get(1).toString
      }
      var lev2_roam_dept: String = ""
      if (!x.isNullAt(2) && !x.get(2).toString.isEmpty) {
        lev2_roam_dept = x.get(2).toString
      }
      var lac_id: String = ""
      if (!x.isNullAt(3) && !x.get(3).toString.isEmpty) {
        lac_id = x.get(3).toString
      }
      var cell_id: String = ""
      if (!x.isNullAt(4) && !x.get(4).toString.isEmpty) {
        cell_id = x.get(4).toString
      }
      var imsi: String = ""
      if (!x.isNullAt(5) && !x.get(5).toString.isEmpty) {
        imsi = x.get(5).toString
      }
      var start_time: String = ""
      if (!x.isNullAt(6) && !x.get(6).toString.isEmpty) {
        start_time = x.get(6).toString
      }
      var call_dur_s: String = ""
      if (!x.isNullAt(7) && !x.get(7).toString.isEmpty) {
        call_dur_s = x.get(7).toString
      }
      (imsi, phone_no, lev1_roam_dept, lev2_roam_dept, lac_id, cell_id, start_time, call_dur_s)
    })
    val outVoiceProcessR: RDD[(String, String, String, Long, Long, Long)] = outVoiceRDD.map(x => {
      // eci处理   cellID处理：去掉|1100，16-10，如果大于65535，取他自己，如果不是取lac_id(16-10)&"-"&cell_id(去掉|1100,16-10)
      var eci = 0L
      val lac_id: String = x._5
      var cellID: String = x._6
      if (cellID.contains("|")) {
        cellID = cellID.split("\\|")(0)
      }
      var cellIDInt: Int = 0
      if (cellID.nonEmpty) {
        cellIDInt = Integer.parseInt(cellID, 16)
      }
      if (cellIDInt <= 65535 && lac_id.nonEmpty) {
        cellIDInt = Integer.parseInt(lac_id, 16) - cellIDInt
      }
      eci = cellIDInt
      // 时间处理
      val sTime: Long = dateFAllLinks.parse(x._7).getTime
      val eTime: Long = sTime + NumberUtils.toLong(x._8)
      (x._1, x._2, x._3 + "\u0001" + x._4, eci, sTime, eTime)

    })

    val outNetFlowRDD: RDD[(String, String, String, String, String, String, String, String)] = hiveContext.sql(outNetFlow).rdd.map(x => {
      var phone_no: String = ""
      if (!x.isNullAt(0) && !x.get(0).toString.isEmpty) {
        phone_no = x.get(0).toString
      }
      var lev1_roam_dept: String = ""
      if (!x.isNullAt(1) && !x.get(1).toString.isEmpty) {
        lev1_roam_dept = x.get(1).toString
      }
      var lev2_roam_dept: String = ""
      if (!x.isNullAt(2) && !x.get(2).toString.isEmpty) {
        lev2_roam_dept = x.get(2).toString
      }
      var lac_id: String = ""
      if (!x.isNullAt(3) && !x.get(3).toString.isEmpty) {
        lac_id = x.get(3).toString
      }
      var cell_id: String = ""
      if (!x.isNullAt(4) && !x.get(4).toString.isEmpty) {
        cell_id = x.get(4).toString
      }
      var imsi: String = ""
      if (!x.isNullAt(5) && !x.get(5).toString.isEmpty) {
        imsi = x.get(5).toString
      }
      var start_time: String = ""
      if (!x.isNullAt(6) && !x.get(6).toString.isEmpty) {
        start_time = x.get(6).toString
      }
      var call_dur_s: String = ""
      if (!x.isNullAt(7) && !x.get(7).toString.isEmpty) {
        call_dur_s = x.get(7).toString
      }
      (imsi, phone_no, lev1_roam_dept, lev2_roam_dept, lac_id, cell_id, start_time, call_dur_s)
    })
    // 数据处理
    val outNetFlowProcessR: RDD[(String, String, String, Long, Long, Long)] = outNetFlowRDD map (x => {

      val sTime: Long = dateFAllLinks.parse(x._7).getTime
      val eTime: Long = sTime + NumberUtils.toLong(x._8)

      (x._1, x._2, x._3 + "\u0001" + x._4, (Integer.parseInt(x._5, 16) * 256 + Integer.parseInt(x._6, 16)).toLong, sTime, eTime)
    })

    // 四者union
    val billProcessR: RDD[(String, String, String, Long, Long, Long)] = localVoiceProcessR.union(localNetFlowProcessR).union(outVoiceProcessR).union(outNetFlowProcessR)

    val billTraceRDD: RDD[Trace] = billProcessR.map(x => {
      val where = new Where()
      where.code = x._4.toString
      where.sTime = x._5
      where.eTime = x._6
      where.lLA = new LLA()
      where.provinceCity = x._3
      (x._1+"\u0001"+x._2, where )
    }).groupByKey()
      .map(x => {
        val sortWheres: List[Where] = x._2.toList.sortBy(_.sTime)
        for (elem <- sortWheres) {
          if (sortWheres.indexOf(elem) != sortWheres.length -1){
            if (elem.eTime > sortWheres(sortWheres.indexOf(elem)+1).sTime){
              elem.eTime = Math.min(elem.eTime,sortWheres(sortWheres.indexOf(elem)+1).sTime)
              sortWheres(sortWheres.indexOf(elem)+1).sTime = Math.max(elem.eTime,sortWheres(sortWheres.indexOf(elem)+1).sTime)
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
        trace.imsi = x._1.split("\u0001")(0)
        trace.date = str_date
        trace.pohoneNum = x._1.split("\u0001")(1)

        trace
      })

    //    imsi,msisdn,imei,lac_cell,rat,longitude,latitude,geohash,country_code,city_code,county_code,enter_time,leave_time,source
    val billTraceR: RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String, String)] = billTraceRDD.flatMap(x => {
      val listBuffer = new ListBuffer[(String, String, String, String, String, String, String, String, String, String, String, String, String, String)]
      x.wheres.foreach(y => {
        val city: String = y.provinceCity.split("\u0001")(0)
        val country: String = y.provinceCity.split("\u0001")(1)
        listBuffer.append((x.imsi, x.pohoneNum, "", y.code.toString, "", "", "", "", "86", city, country, y.sTime.toString, y.eTime.toString, "bill"))
      })
      listBuffer
    })

    val resultRow: RDD[Row] = billTraceR.map(x => Row(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9, x._10, x._11, x._12, x._13, x._14))
    val schema = "imsi,msisdn,imei,lac_cell,rat,longitude,latitude,geohash,country_code,city_code,county_code,enter_time,leave_time,source"
    val structType: StructType = StructType(schema.split(",").map(x => StructField(x, StringType, true)))
    val dataFrame: DataFrame = hiveContext.createDataFrame(resultRow, structType)
    dataFrame.createOrReplaceTempView("tmpTable")
    val insertSQL: String = s"insert overwrite table ${outputDataBse}.${outputTable} partition(prodate='${str_date}',hour_id='${str_hour}') " +
      "select imsi,msisdn,imei,lac_cell,rat,longitude,latitude,geohash,country_code,city_code,county_code,enter_time,leave_time,source from tmpTable"
    println(insertSQL)
    hiveContext.sql(insertSQL)

    sc.stop()
    println("success!————话单解析" + str_date)


  }
}


























