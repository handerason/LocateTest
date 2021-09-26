package com.bonc.user

import com.bonc.location.{LocType, Location}
import com.bonc.location.LocType.LocType
import com.bonc.network.NetworkType
import com.bonc.trace.Trace

import scala.collection.immutable
import scala.collection.immutable.HashMap

/**
  * 人群,由用户构成
  */
class Crowd() extends Serializable {
  var users: immutable.HashMap[String, User] = new immutable.HashMap[String, User]; //key=imsi, value = user

  /**
   * 人群中的用户人头数
   * @return 用户数量
   */
  def headcount(): Int ={
    users.size
  }

  override def toString: String = {
    val builder = new StringBuilder
    for (i <- users){
      builder.append(i._1+","+i._2.toString+"|")
    }
    builder.toString()
  }

  /**
   * 判断人群是否为空
   * @return
   */
  def isEmpty(): Boolean ={
    if(users.nonEmpty) false
    else               true
  }
  /**
   * 判断人群是否为空
   * @return
   */
  def nonEmpty(): Boolean ={
    if(users.nonEmpty) true
    else               false
  }

  /**
    * 过滤保留居住地的用户
    * @param workingPlace 工作地
    */
  def retainWorkUsers(workingPlace: Location,locType: LocType): Map[String, User] ={
    var filterMap: Map[String, User] = new HashMap[String, User]
    if (locType.equals(LocType.AOI)){
      filterMap = users.filter(entry => entry._2.workPlace._1.equals(workingPlace.code))
    }else if (locType.equals(LocType.GRID)){
      filterMap = users.filter(entry => entry._2.workPlace._2.equals(workingPlace.code))
    }else if (locType.equals(LocType.SECTOR)){
      filterMap = users.filter(entry => entry._2.workPlace._3.equals(workingPlace.code))
    }
    filterMap
  }

  /**
    * 过滤保留工作地的用户
    * @param livingPlace 特定位置
    */
  def retainLivingUsers(livingPlace: Location,locType: LocType): Map[String, User] ={
    var filterMap: Map[String, User] = new HashMap[String, User]
    if (locType.equals(LocType.AOI)){
      filterMap = users.filter(entry => entry._2.liviPlace._1.equals(livingPlace.code))
    }else if (locType.equals(LocType.GRID)){
      filterMap = users.filter(entry => entry._2.liviPlace._2.equals(livingPlace.code))
    }else if (locType.equals(LocType.SECTOR)){
      filterMap = users.filter(entry => entry._2.liviPlace._3.equals(livingPlace.code))
    }
    filterMap
  }

  /**
    * 提取用户居住地为特定位置的所有用户，生成新的Crowd
    *
    * @param livingPlace 特定位置
    */
  def subCrowdByLive(livingPlace: Location,locType: LocType): Crowd = {
    
    val resCrowd = new Crowd()
    users.foreach(entry => {
      var secCode = ""
      if (locType.equals(LocType.AOI)){
        secCode = entry._2.liviPlace._1
      }else if (locType.equals(LocType.GRID)){
        secCode = entry._2.liviPlace._2
      }else if (locType.equals(LocType.SECTOR)){
        secCode = entry._2.liviPlace._3
      }
      if (secCode.equals(livingPlace.code)) {
        resCrowd.add(entry._2)
      }
    })
    resCrowd
  }

  /**
    * 提取用户工作地为特定位置的所有用户，生成新的Crowd
    *
    * @param workingPlace 特定位置
    */
  def subCrowdByWork(workingPlace: Location,locType: LocType): Crowd = {
    
    val resCrowd = new Crowd()
    users.foreach(entry => {
      var secCode = ""
      if (locType.equals(LocType.AOI)){
        secCode = entry._2.liviPlace._1
      }else if (locType.equals(LocType.GRID)){
        secCode = entry._2.liviPlace._2
      }else if (locType.equals(LocType.SECTOR)){
        secCode = entry._2.liviPlace._3
      }
      if (secCode.equals(workingPlace.code)) {
        resCrowd.add(entry._2)
      }
    })
    resCrowd
  }

  /**
    * 提取用户居住地和工作地为特定位置的所有用户，生成新的Crowd
    *
    * @param livingPlace  特定位置
    * @param workingPlace 特定位置
    */
  def subCrowd(livingPlace: Location, workingPlace: Location,locType: LocType): Crowd = {

    val resCrowd = new Crowd()
    users.foreach(entry => {
      var homeCode = ""
      var workCode = ""
      if (locType.equals(LocType.AOI)){
        homeCode = entry._2.liviPlace._1
        workCode = entry._2.workPlace._1
      }else if (locType.equals(LocType.GRID)){
        homeCode = entry._2.liviPlace._2
        workCode = entry._2.workPlace._2
      }else if (locType.equals(LocType.SECTOR)){
        homeCode = entry._2.liviPlace._3
        workCode = entry._2.workPlace._3
      }
      if (homeCode.equals(workingPlace.code) && homeCode.equals(workingPlace.code)) {
        resCrowd.add(entry._2)
      }
    })
    resCrowd
  }

  /**
    * 增加一个用户
    *
    * @param user 待增加的用户
    */
  def add(user: User): Unit = {
    users += (user.imsi -> user)
  }

  /**
    * 洞察人群男女性别个数及比例
    */
  def insightGender(): (Int, Int, Double, Double) = {
    var maleCnt: Int = 0 //男性人数计数
    var femaleCnt: Int = 0 //女性人数计数
    var maleRatio: Double = 0 //男性人数比例
    var femaleRatio: Double = 0 //女性人数比例
    for (user <- users.values) {
      if (user.gender == Gender.Male) maleCnt += 1
      else femaleCnt += 1
    }
    val headCnt: Double = users.size //总人数
    //计算男女人口比例，如果总人数为0，则都为0
    if (headCnt > 0) {
      maleRatio = maleCnt / headCnt
      femaleRatio = femaleCnt / headCnt
    }
    (maleCnt, femaleCnt, maleRatio, femaleRatio)
  }

  /**
    * 洞察人群ARPU总数
    */
  def insightArpu(): Double = {
    var totArpu: Double = 0.0
    for (user <- users.values) {
      totArpu += user.arpu
    }
    totArpu
  }

  /**
   * (改) 洞察5G业务的用户业务总数以及占比
   * @return
   */
  def insight5GService(): (Int, Double) = {
    var serviceNum = 0
    var g5Cnt: Int = 0 //5G业务数量
    for (user <- users.values) {
      serviceNum += 1
      if (user.service.networkType != null){
        if (user.service.networkType.equals(NetworkType.G5)) g5Cnt += 1
      }
    }
    println(serviceNum)
    println(g5Cnt)
    val percentage: Double = g5Cnt.toDouble / serviceNum.toDouble
    (g5Cnt,percentage)
  }

  def insight5GAT(): (Int, Double) = {
    (0,0D)
  }

  /**
    * 人群的某段时间内平均从居住地到工作地的通勤时间
    * @param traces 用户从Home到工作地的足迹映射，List[Trace]是这个用户一段时间内的足迹,String是imsi
    * @return 人群通勤平均时间
    */
  def avgHome2WorkTime(traces: Map[String, List[Trace]],locType: LocType): Int = {
    var time = 0
    var count = 0
    val trace = new Trace()
    for (user <- users.values) {
      if (traces.keys.toList.contains(user.imsi)){
        time += trace.avgHome2WorkTime(traces(user.imsi), user,locType)
        count += 1
      }
    }
    time / count
  }

  /**
    * 人群的某段时间内平均从工作地到居住地的通勤时间
    * @param traces 用户从工作地到居住地的足迹集合，List[Trace]是这个用户一段时间内的足迹,String是imsi
    * @return 人群通勤平均时间
    */
  def avgWork2HomeTime(traces: Map[String, List[Trace]],locType: LocType): Int ={
    var time = 0
    var count = 0
    val trace = new Trace()
    for(user <- users.values) {
      if (traces.keys.toList.contains(user.imsi)) {
        time += trace.avgWork2HomeTime(traces(user.imsi), user, locType)
        count += 1
      }
    }
    time / count
  }
}
