package com.bonc.time

import java.text.SimpleDateFormat
import java.util.Date

/**
 * 居家时间（默认为0-6，22到0两个时间段）
 *
 *  eTime1 0点-sTime为第一个时间段
 *  sTime2 eTime-0点为第二个时间段
 */
// 这个方法应该是居家开始时间是22点，居家结束时间是6点
case class Home() extends Serializable {
  var eHour: Double = 22 //0点-eHour1为第一个时间段
  var sHour: Double = 6 //sHour2-0点为第二个时间段
  var minStatDays: Int = 365 //最少统计天数
  var minValidRadio: Double = 0.5 //有效日期占50%

  /**
   * 是否在居家时间内
   * @param time 时间
   * @return 是/否
   */
  def at(time: Long) : Boolean ={
    //转换成小时
    val date = new Date(time)
    val hour = date.getHours
    if(hour <= sHour || hour >= eHour)  true
    else                                  false
  }
}
