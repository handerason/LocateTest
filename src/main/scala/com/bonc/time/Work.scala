package com.bonc.time

import java.text.SimpleDateFormat
import java.util.Date

/**
  * 工作时间(默认为8点到17点)
 *
 *  sHour sHour时间段的开始
  *  eHour eHour时间段的结束
  */
case class Work() extends Serializable {

  var sHour: Double = 8.0 //sHour时间段的开始
  var eHour: Double = 17.0 //eHour时间段的结束
  var minDuration: Double = 1.0 //超过1小时
  var minStatDays: Int = 365 //最少统计天数
  var minValidRadio: Double = 0.5 //有效日期占50%
  /**
    * 绝对开始时间，利用当天0点绝对时间+工作开始时间
    * @param strUTF 年月日
    * @return 工作开始绝对时间
    */
  def sTime(strUTF: String): Long ={
    var minute = ""
    var hour = ""
    //转换后+sHour，再转换
    val dateFormat = new SimpleDateFormat("yyyyMMddHHmm")
    val mintueTmp: Int = ((sHour % 1.0) * 60).toInt
    if (mintueTmp < 10){
      minute = "0" +mintueTmp
    }
    val hourTmp: Int = (sHour - (sHour % 1.0)).toInt
    if (hourTmp<10){
      hour = "0" + hourTmp
    }else{
      hour = hourTmp.toString
    }
    val stime: Long = dateFormat.parse(strUTF + hour + minute).getTime
    stime
  }
  /**
    * 绝对结束时间，利用当天0点绝对时间+工作结束时间
    * @param strUTF 年月日
    * @return 工作结束绝对时间
    */
  def eTime(strUTF: String): Long ={
    var minute = ""
    var hour = ""
    //转换后+sHour，再转换
    val dateFormat = new SimpleDateFormat("yyyyMMddHHmm")
    val mintueTmp: Int = ((eHour % 1.0) * 60).toInt
    if (mintueTmp < 10){
      minute = "0" +mintueTmp
    }
    val hourTmp: Int = (eHour - (eHour % 1.0)).toInt
    if (hourTmp < 10){
      hour = "0" + hourTmp
    }else{
      hour = hourTmp.toString
    }
//    println(hour)
    val etime: Long = dateFormat.parse(strUTF + hour + minute).getTime
    etime
  }
  /**
   * 在工作时间片段的截取，如果没有在工作时间，则为0
   * @param sTime 开始时间
   * @param eTime 结束时间
   * @return 在工作时间
   */
  def splitAt(sTime: Long, eTime: Long): Double ={
    //转换成小时
    val sDate = new Date(sTime)
    val eDate = new Date(eTime)
    val sDateHour: Int = sDate.getHours
    val eDateHour: Int = eDate.getHours
    var workDuartion = 0L
//    println(sDateHour+"-"+sHour+"-"+eDateHour+"-"+eHour)
    if (sDateHour > sHour && eDateHour < eHour){
//      println("xxxxxxxxxxx")
      workDuartion = eTime - sTime
    }else{
      workDuartion = 0
    }
////    println(sDateHour+"-"+eDateHour)
////    println("sDateHour"+sDateHour)
////    println("eDateHour"+eDateHour)
//    val sHour1 = Math.max(sHour, sDateHour)
////    println("sHour1:"+sHour1)
//    val eHour1 = Math.min(eHour, eDateHour)
////    println("eHour1:"+eHour1)
//    //返回At Work时间片段或者0
//    Math.max(eHour1 - sHour1, 0)
    workDuartion.toDouble
  }
}
