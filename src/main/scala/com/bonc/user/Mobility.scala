package com.bonc.user

import java.text.SimpleDateFormat
import java.util.Date

import com.bonc.location.Location
import com.bonc.trace.Trace

import scala.collection.immutable

/**
 * 人口的流动性，包括常驻人口、流动人口、过境人口和未到此过
 */
object Mobility extends Enumeration with Serializable {
  type Mobility = Value
  val INHABITANT, MIGRANT, TRANSIT, ABSENT, LessDate = Value //常驻人口、流动人口、过境人口、未到此过和日期数不合格
  var allDays:Int = 180
  var leastDays: Int = 7
  /**
   * 根据用户1年的轨迹识别用户是否为常驻用户，如果超过6个月的时间在当前位置则认为是当前位置的常住用户（改）
   *
   * @param traces     输入用户超过一年的trace[日期，Trace]
   * @param location   用户的流动性是相对每个位置区域而言的
   * @param inhDaysThr , migDaysThr常驻人口和流动人口日期门限
   * @param statisticsDays,进行统计的天数的最低要求，最少需要多少天才使数据有效
   * @return true:常驻用户，false：非常驻用户（改：返回的是Mobility的枚举类型）
   */
  def recognize(traces: immutable.TreeMap[String, Trace], location: Location,inhDaysThr:Int = allDays,migDaysThr:Int = leastDays,statisticsDays:Int=365): Option[Mobility.Value] = {
    //判断考察时间是否为1年（360天，大于1年），最大日期-最小日期 < 365或者>367，则不处理
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val sortTree: Map[Date, Trace] = traces.map(x => {
      val date: Date = dateFormat.parse(x._1)
      (date, x._2)
    }).toList.sortBy(_._1).toMap
//    println("sortTree"+sortTree)

    val minTime: Long = sortTree.keys.head.getTime
    val maxTime: Long = sortTree.keys.last.getTime
    val totalDay: Long = (maxTime - minTime) / (1000 * 3600 * 24)
//    println(totalDay)
    // 这里是统计是否需要一年的数据
    if (totalDay < statisticsDays || totalDay > 367) {
      return Some(LessDate)
    }

//    println("filterTree"+filterTree)
//    println(migDaysThr)
    //计算经过本地的天数
    var validDays = 0
    sortTree.foreach((trace: (Date, Trace)) => {
      //足迹是否经过过境本位置次数大于0
//      if (com.bonc.trace._2.passby(location) > 0)
      if (trace._2.contains(location.code))
        validDays += 1
    })
//    println(validDays)
    //判断用户移动性
    if (validDays > inhDaysThr) Some(INHABITANT)  //全年出现天数大于 6 个月的用户定义为常住人口
    else if (validDays > migDaysThr) Some(MIGRANT)  //全年出现天数大于 7天的用户定义为流动人口
    else if (validDays > 0) Some(TRANSIT) //大于1天，小于7天
    else Some(ABSENT) //从为来过

  }
}
