package com.bonc.user

import com.bonc.lla.LLA
import com.bonc.location.LocType
import com.bonc.location.LocType.LocType
import com.bonc.network.NetworkType
import com.bonc.time.{Home, Work}
import com.bonc.user.Gender.Gender
import com.bonc.data.Scene
import com.bonc.trace.{Trace, Where}

import scala.collection.immutable.TreeMap
import scala.collection.mutable.ListBuffer
import scala.collection.{immutable, mutable}
import scala.util.control.Breaks.breakable

/**
  * 对用户属性的描述或者定义
  */
class User extends Serializable {
  var gender: Gender = _ //用户性别
  var age : String = _ //年龄描述，可能是年龄段等
  var imsi: String = _ //终端IMSI
  var phoneNumber: String = _ //电话号码
  var arpu: Double = 0 //用户收入
  var service: Service = new Service //用户办理的业务
  var liviPlace: (String, String, String)  = ("","","") //居住地(Aoi, Grid, Sector)
  var workPlace: (String, String, String) = ("","","") //工作地(Aoi, Grid, Sector)
  /**
   * 提取居住地编码
   * @param locType 位置类型
   * @return
   */
  def getLiviPlace(locType: LocType): Option[String] ={
    locType match{
      case LocType.AOI => Some(liviPlace._1)
      case LocType.GRID => Some(liviPlace._2)
      case LocType.SECTOR => Some(liviPlace._3)
      case _ => None
    }
  }

  override def toString: String = {
    imsi+","+gender+","+age+","+phoneNumber+","+arpu+","+liviPlace+","+workPlace
  }
  /**
   * 提取工作地编码
   * @param locType 位置类型
   * @return
   */
  def getWorkPlace(locType: LocType): Option[String] ={
    locType match{
      case LocType.AOI => Some(workPlace._1)
      case LocType.GRID => Some(workPlace._2)
      case LocType.SECTOR => Some(workPlace._3)
      case _ => None
    }
  }
  /**
   * 基于一个用户的常住地表和用户标签表内容生成一个用户对象
   */
  def create(workliveTab: String, tagTab: String): Unit ={
    //内容根据实际情况填写
  }

  /**
   * 在用户多天的trace中寻找用户居住地
   * 一个月内日居住地稳定出现天数超过 50%，则标记为用户居住地
   * @param traces 用户多天的trace[日期， Trace]
   */
  def updateLiviPlace(traces: immutable.TreeMap[String,Trace],home: Home): Unit = {
    if (traces.size > home.minStatDays) {
      val traceType: LocType = traces.values.head.traceType //轨迹类型
      val validDays = new mutable.HashMap[String, Int]() //记录有效时间
      breakable {
        traces.foreach(trace => {
          trace._2.wheres.foreach(where => {
            if (home.at(where.sTime) || home.at(where.eTime)) {
              val vDays = validDays.get(where.code)
              vDays match {
                case Some(days) => validDays.put(where.code, days.+(1))
                case None => validDays.put(where.code, 1)
              }
//              break() // ???
            }
          })
        })
      }
//      println(validDays)
      var place: String = "" //
      if (validDays.nonEmpty){
        //找到天数最多的位置，且满足最大比例条件就认为找到，否则认为没有找到
        val maxDays: (String, Int) = validDays.maxBy(_._2)
        //      println(maxDays.toString())

        if (maxDays._2 / traces.size.toDouble > 0.5) {
          place = maxDays._1
        }
      }
//      println(place)
      traceType match { //根据不同类型存放不同位置
        case LocType.AOI => liviPlace = (place, liviPlace._2, liviPlace._3)
        case LocType.GRID => liviPlace = (liviPlace._1, place, liviPlace._3)
        case LocType.SECTOR => liviPlace = (liviPlace._2, liviPlace._3, place)
      }
    }
  }
  /**
   * 在用户多天的trace中寻找用户工作地
   * @param traces 用户多天的trace[日期， Trace]
   */
  def updateWorkPlace(traces: immutable.TreeMap[String,Trace],work: Work): Unit = {
    //一个月内日工作地稳定出现(超过1小时)天数超过 50%，则标记为用户工作地
    val traceType = traces.values.head.traceType //轨迹类型
    val validDays = new mutable.HashMap[String, Int]() //记录有效时间
    if (traces.size > work.minStatDays) {
      breakable {
        traces.foreach(trace => {
          //保存在工作时间片段
          val durations = new mutable.HashMap[String, Double]() //记录在某位置的时长
          trace._2.wheres.foreach(where => {
            //统计用户经历每个位置的时间片段集合
//            println(where.eTime-where.sTime)
            val timeSplit = work.splitAt(where.sTime, where.eTime)
//            println(timeSplit)
//            val duration = durations.get(where.code)
//            duration match {
//              case Some(during) => durations.put(where.code, during + timeSplit)
//              case None => durations.put(where.code, timeSplit)
//            }
            if (durations.keys.toList.contains(where.code)){
              val duration: Double = durations(where.code)
              durations.put(where.code, duration + timeSplit)
            }else{
              durations.put(where.code, timeSplit)
            }
          })
//          println(durations.filter(!_._2.equals(0.0)))
          //如果时间超过60分钟，则计数本日
          durations.foreach(duration => {
//            println(durations)
            if (duration._2 > work.minDuration) {
              val vDays = validDays.get(duration._1)
              vDays match {
                case Some(days) => validDays.put(duration._1, days.+(1))
                case None => validDays.put(duration._1, 1)
              }
            }
          })
        })
      }
//      println(validDays)
      //找到天数最多的位置，且满足最大比例条件就认为找到，否则认为没有找到
      var place: String = "" //
      if (validDays.nonEmpty){
        val maxDays = validDays.maxBy(_._2)
        if (maxDays._2 / traces.size.toDouble > 0.5) {
          place = maxDays._1
        }
      }
      traceType match { //根据不同类型存放不同位置
        case LocType.AOI => workPlace = (place, workPlace._2, workPlace._3)
        case LocType.GRID => workPlace = (workPlace._1, place, workPlace._3)
        case LocType.SECTOR => workPlace = (workPlace._2, workPlace._3, place)
      }
    }
  }
}


object User{
  def main(args: Array[String]): Unit = {
    val user = new User
    user.gender = Gender.Male
    user.age = "10-15"
    user.imsi = "user"
    user.phoneNumber = "13011112222"
    user.arpu = 1245D
    val service = new Service
    service.networkType = NetworkType.G5
    user.service = service
    user.liviPlace = ("aoi1","140~240","10256")
    user.workPlace = ("aoi2","150~250","10257")

    println(user.getWorkPlace(LocType.GRID))

//    val loc1 = new Where()
//    loc1.lLA = new LLA(112, 34)
//    loc1.sTime = 1599660691000L
//    loc1.eTime = 1599660792000L
//    loc1.code = "1111"
//    loc1.scene = Scene.INDOOR
//    val loc2 = new Where()
//    loc2.lLA = new LLA(111, 30)
//    loc2.sTime = 1599660793000L
//    loc2.eTime = 1599660892000L
//    loc2.code = "1122"
//    loc2.scene = Scene.INDOOR
//    val loc3 = new Where()
//    loc3.lLA = new LLA(115, 32)
//    loc3.sTime = 1599660992000L
//    loc3.eTime = 1599661092000L
//    loc3.code = "11333"
//    loc3.scene = Scene.INDOOR
//    val loc4 = new Where()
//    loc4.lLA = new LLA(121, 31)
//    loc4.sTime = 1599661292000L
//    loc4.eTime = 1599661492000L
//    loc4.code = "1114"
//    loc4.scene = Scene.INDOOR
//    val loc5 = new Where()
//    loc5.lLA = new LLA(121, 31)
//    loc5.sTime = 1599661692000L
//    loc5.eTime = 1599662292000L
//    loc5.code = "1114"
//    loc5.scene = Scene.INDOOR
//
//    val wheres = new ListBuffer[Where]()
//    wheres.add(loc1)
//    wheres.add(loc2)
//    wheres.add(loc3)
//    wheres.add(loc4)
//    wheres.add(loc5)
//
//    val com.bonc.trace = new Trace(LocType.AOI)
//    com.bonc.trace.date = "20200909"
//    com.bonc.trace.imsi = "imsi1"
//    com.bonc.trace.wheres = wheres
//
//    var stringTrace = new TreeMap[String, Trace]
//    stringTrace += (com.bonc.trace.imsi -> com.bonc.trace)
//    val home = new Home
//    home.eHour1 = 22D
//    home.sHour2 = 6D
//    home.minStatDays = 0
//    home.minValidRadio = 0.5
//    val user1 = new User
//    user1.updateLiviPlace(stringTrace,home)
//    println(user1.toString)

    val loc1 = new Where()
    loc1.lLA = new LLA(112, 34)
    loc1.sTime = 1599624691000L
    loc1.eTime = 1599628891000L
    loc1.code = "1111"
    loc1.scene = Scene.INDOOR
    val loc2 = new Where()
    loc2.lLA = new LLA(111, 30)
    loc2.sTime = 1599628891000L
    loc2.eTime = 1599629591000L
    loc2.code = "1122"
    loc2.scene = Scene.INDOOR
    val loc3 = new Where()
    loc3.lLA = new LLA(115, 32)
    loc3.sTime = 1599629591000L
    loc3.eTime = 1599636591000L
    loc3.code = "11333"
    loc3.scene = Scene.INDOOR
    val loc4 = new Where()
    loc4.lLA = new LLA(121, 31)
    loc4.sTime = 1599636591000L
    loc4.eTime = 1599643691000L
    loc4.code = "1114"
    loc4.scene = Scene.INDOOR
    val loc5 = new Where()
    loc5.lLA = new LLA(121, 31)
    loc5.sTime = 1599643691000L
    loc5.eTime = 1599652791000L
    loc5.code = "1114"
    loc5.scene = Scene.INDOOR

    val wheres = new ListBuffer[Where]()
    wheres.append(loc1)
    wheres.append(loc2)
    wheres.append(loc3)
    wheres.append(loc4)
    wheres.append(loc5)

    val trace = new Trace(LocType.AOI)
    trace.date = "20200909"
    trace.imsi = "imsi1"
    trace.wheres = wheres

    var stringTrace = new TreeMap[String, Trace]
    stringTrace += (trace.imsi -> trace)
    val work = new Work
    work.sHour = 10D
    work.eHour = 20D
    work.minDuration = 0D
    work.minStatDays = 0
    work.minValidRadio = 0.1
    val user1 = new User
    user1.updateWorkPlace(stringTrace,work)
    println(user1.toString)

  }
}

















