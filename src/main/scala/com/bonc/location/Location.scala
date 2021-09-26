package com.bonc.location

import com.bonc.lla.LLA
import com.bonc.location.LocType.LocType
import com.bonc.od.Od
import com.bonc.time.Work
import com.bonc.user.Mobility.Mobility
import com.bonc.user.{Crowd, Mobility, User}
import com.bonc.trace.{Trace, Where}

import scala.collection.immutable.TreeMap


/**
  * 标识位置的类，是Aoi, Sector和Grid统称，也是它们的超类
  */
class Location extends Serializable {
  var lLA: LLA = _ //中心坐标（对于小区来说是小区坐标）
  var code: String = _ //编号
  var crowd: Crowd = _ //区域包含的人群,使用时可以用来表示工作人口、居住人口和经留人口等

  def area(): Double= {
    ???
  } //计算位置的面积
  def contains(lLA: LLA): Boolean= {
    ???
  } //坐标是否归属本位置
  def containsLoc(otherLoc: Location): Boolean = {
    ???
  } //该位置是否包含另外一个位置

  override def toString: String = {

    lLA.toString+","+code+","+crowd.toString

  }
  override def equals(obj: Any): Boolean = {
    if(!obj.isInstanceOf[Location]) {
      false
    }else {
      val x = obj.asInstanceOf[Location]
      this.code == x.code; //编码相同
      }
  }
  /**
    * 计算得到两个位置的曼哈顿距离
    * @param location 结束位置
    * @return 曼哈顿距离
    */
  def getMhDistance(location: Location): Double ={
    lLA.getMhDistance(location.lLA)
  }
  /**
    * 计算得到两个位置的欧几里得距离
    * @param location 结束位置
    * @return 欧几里得距离
    */
  def getDistance(location: Location): Double ={
    lLA.getDistance(location.lLA)
  }

  /**
   * 只保留本地常住用户
   */
  def remainLiviCrowd(locType: LocType): Map[String, User] ={
    crowd.retainLivingUsers(this, locType)
  }
  /**
    * 从用户集中提取居住地为location的code的用户并生成新的Crowd
    * @return 居住地用户群体
    */
  def makeLiviCrowd(): Crowd ={
    val liviCrowd = new Crowd
    crowd.users.foreach(user => {
      if(user._2.liviPlace._1.eq(code) ||user._2.liviPlace._2.eq(code) || user._2.liviPlace._3.eq(code) ){
        liviCrowd.add(user._2)
      }
    })
    liviCrowd
  }
  /**
    * 从用户集中提取工作地用户并生成新的Crowd
    * @return 工作地用户群体
    */
  def makeWorkCrowd(): Crowd ={
    val workCrowd = new Crowd
    crowd.users.foreach(user => {
      if(user._2.workPlace._1.eq(code) ||user._2.workPlace._2.eq(code) || user._2.workPlace._3.eq(code)){
        workCrowd.add(user._2)
      }
    })
    workCrowd
  }
  /**
    * 从用户集中提取流动人口用户并生成新的Crowd(改)
    * @return 工作地用户群体
    * @example 这个方法其实传入的traces为[用户imsi，[用户trace的日期，用户的trace]],
   *          这个存储的一段时间内的不同用户的trace，也就是轨迹集合，
   *          整个结构的存储也可以是同一用户，不同天的数据,然后根据Location的指定位置编码，来判断用户在此地的天数，
   *          判断是否大于migDaysThr，来判断用户在此地是流动人口
    */
  def makeMigrantCrowd(traces:Map[String, TreeMap[String, Trace]], inhDaysThr:Int = 180, migDaysThr: Int=7,statisticsDays:Int=365): Crowd ={
    val migCrowd = new Crowd

    crowd.users.foreach(user => {
      var flag: Int = -1
      val userTraces: Option[TreeMap[String, Trace]] = traces.get(user._2.imsi)
//      println(userTraces)
      userTraces match{
        case Some(userTraces) => flag = isMigrant(userTraces,inhDaysThr, migDaysThr,statisticsDays)
        case None => flag = -1
      }
//      println(flag)
      if(flag == 1){//该错误怎么处理？
        migCrowd.add(user._2)
      }
    })
    migCrowd
  }
  /**
    * 根据用户足迹判断用户是否为常驻用户
    * @return 1:是 0:否, -1:未识别出来
    */
  def isInhabitant(traces: TreeMap[String,Trace], inhDaysThr:Int = 180, migDaysThr: Int=7,statisticsDays:Int=365): Int ={
    val mobility:Option[Mobility.Value] = Mobility.recognize(traces, this,inhDaysThr,migDaysThr,statisticsDays)
//    println("mobility"+mobility)
    mobility match{
      case Some(Mobility.INHABITANT) => 1
      case None => -1
      case _ => 0
    }
  }
  /**
    * 根据用户足迹判断用户是否为流动用户
    * @return 1:是 0:否, -1:未识别出来
    */
  def isMigrant(traces: TreeMap[String,Trace], inhDaysThr:Int = 180, migDaysThr: Int=7,statisticsDays:Int=365): Int ={
    val mobility:Option[Mobility] = Mobility.recognize(traces, this, inhDaysThr, migDaysThr,statisticsDays)
//    println(mobility)
    mobility match{
      case Some(Mobility.MIGRANT) => 1
      case None => -1
      case _ => 0
    }
  }

  /**
    * 生成出行到外地的人群（常住地在本地）
    * @param traces 用户轨迹（IMSI+TRACE），一个用户1天的数据
    * @param work 这个是生成居住地在本地，出去上班的用户群体
    */
  def makeCrowdTravelOut(traces:Map[String, Trace],locType: LocType,work: Work): Crowd ={
   //经历过外地旅行的用户群存放
    val outCrowd = new Crowd
    //生成常住地人群
    val liviCrowd = this.makeLiviCrowd()
    for((imsi, user) <- liviCrowd.users){
//      println((imsi, user))
      //提取trace，根据IMSI
      val trace: Option[Trace] = traces.get(imsi)
//      println(trace)
      if(trace.isDefined){
        // 计算一天的OD，获取从居住地出发到经留时间最长的工作地的出行OD
        val od: Option[Od] = trace.get.get1DayOd(user,60,locType,work)
        if(od.isDefined){
          outCrowd.add(user)
        }
      }
    }
    outCrowd
  }
  /**
   *生成当天到指定地点的用户群（常住地是本地）
   * @param dLoc 指定目的地
   * @return 用户群
   */
  def makeCrowd2D(dLoc: Location,traces:Map[String, Trace],locType: LocType,work: Work): Crowd ={
    //经历过外地旅行的用户群存放
    val dLocCrowd = new Crowd
    //生成常住地人群
    val liviCrowd = this.makeLiviCrowd()
    for((imsi, user) <- liviCrowd.users){
      //提取trace，根据IMSI
      val trace = traces.get(imsi)
      if(trace.isDefined){
        val od: Option[Od] = trace.get.get1DayOd(user,60,locType,work)
        if(od.isDefined && od.head.dLoc.code.eq(dLoc.code)){
          dLocCrowd.add(user)
        }
      }
    }
    dLocCrowd
  }
  /**
    * 判断用户是否为流动人口（改）
    * @param user 用户
    * @param trace 每日轨迹集合
    * @return 是/否
    */
  def isTransient(user: User, trace: Trace,timeLimit:Int = 6): Boolean ={
    val limit: Int = timeLimit * 3600000
    var costTime = 0L
    var flag = false
    val maybeUser: Option[User] = crowd.users.get(user.imsi)
    maybeUser match {
      case Some(user) => if (trace.contains(code)){
        val wheres: List[Where] = trace.getWheres(code)
        for (i <- wheres){
          costTime += i.duration.toLong
        }

        // 在此地驻留时长不超过6小时，被视为流动人口
        if (costTime < limit){
          flag = true
        }else{
          flag = false
        }
      }
      case None => flag = false
    }
    flag
  }

  /**
    * 从用户全集中提取居住地属于本位置的用户
    * @param users 用户全集
    */
  def extract2LivingCrowd(users: List[User]): Crowd ={
    val crowd_result = new Crowd
    users.foreach(user => {
      if(user.liviPlace._1.equals(code) || user.liviPlace._2.equals(code) || user.liviPlace._3.equals(code)){
        crowd_result.add(user)
      }
    })
    crowd_result
  }

    /**
      * 从用户全集中提取工作地属于本位置的用户
      * @param users 用户全集
      */
    def extract2WorkingCrowd(users: List[User]): Crowd ={
      val crowd_result = new Crowd
      users.foreach(user => {
        if(user.workPlace._1.equals(code) || user.workPlace._2.equals(code) || user.workPlace._3.equals(code)){
          crowd_result.add(user)
        }
      })
      crowd_result
    }

  /**
    * 从用户全集中提取某时间段曾经留于本位置的用户
    * @param users 用户全集,key为IMSI，Value为user
    * @param traces 用户轨迹全集，key为IMSI，Value为trace
    * @param sTime 开始时间
    * @param eTime 结束时间
    * @deprecated (这里修改了getLocations这个方法，这个是新加的，没有用Location，用的Where)
    */
  def makeLingerCrowd(users: Map[String, User], traces: Map[String, Trace],
                     sTime: Long, eTime: Long): Crowd ={
    val crowd_result = new Crowd
    for((imsi, user) <- users){
      //提取trace和满足要求的位置
      if (traces.keys.toList.contains(imsi)){
        val trace: Trace = traces(imsi)
        trace.getLocations(code).foreach((location: Where) => {
          //用户改位置驻留时间与统计时间有重叠
          if(location.sTime < eTime && location.eTime > sTime){
            crowd_result.add(user)
          }
        })
      }
    }
    crowd_result
  }

  /**
    * 区域的总人数
    * @return 总人数
    */
  def headCount(): Int = {
    crowd.headcount()
  }
  /**
    * 区域的人口密度，如果area为0，则返回密度为PositiveInfinity
    * @return 总人口密度
    */
  def headDensity(): Double = {
    if(area>0.000001) crowd.users.size/area
    else              Double.PositiveInfinity
  }

  /**
    * 人群的平均从居住地到工作地的通勤时间
    *
    * @param traces 用户从Home到工作地的足迹集合
    * @return 人群通勤平均时间
    */
  def avgHome2WorkTime(traces: Map[String, List[Trace]],locType: LocType): Int ={
    crowd.avgHome2WorkTime(traces,locType)
  }

  /**
    * 人群的平均从工作地到居住地的通勤时间
    *
    * @param traces 用户从工作地到居住地的足迹集合
    * @return 人群通勤平均时间
    */
  def avgWork2HomeTime(traces: Map[String, List[Trace]],locType: LocType): Int ={
    crowd.avgWork2HomeTime(traces,locType)
  }
}
