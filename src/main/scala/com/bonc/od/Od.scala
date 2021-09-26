package com.bonc.od

import com.bonc.location.Location
import com.bonc.user.{Crowd, User}
import com.bonc.trace.{Trace, Where}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * O位置与D位置构成的OD对，OD对象一般是描述用户群体行为，而不是用户轨迹中的OD节点
 */
class Od extends Serializable {
  var oLoc: Location = new Location //源位置
  var dLoc: Location = new Location //目的位置
  var crowd: Crowd = new Crowd //从O到D用户群
  var behaviors: mutable.HashMap[String, (Int, Double, Double)] = new mutable.HashMap[String, (Int, Double, Double)] //OD间用户行为参数：标识/时间/欧氏距离/街道距离，拥有直方图统计等
  var distance: Double = 0.0 //oD平均欧几里得距离
  var mHDistance: Double = 0.0 //od平均曼哈顿x距离
  var time: Int = 0 //用户从O到D平均耗时

  def this(oLoc: Location, dLoc: Location){
    this()
    this.oLoc = oLoc
    this.dLoc = dLoc
  }

  override def toString: String = {

    oLoc.toString+","+dLoc.toString+","+crowd.toString+","+behaviors.toString()+","+distance+mHDistance+time

  }

  /**
   * 利用多个用户ODWheres更新Od
   * @param odWheres 多个用户ODWheres
   */
  def updateOd(odWheres: List[(String, Where, Where)]): Unit ={
    for(odWhere <- odWheres) {
      updateOd(odWhere._1, odWhere._2, odWhere._3)
    }
  }
  /**
   * 利用用户轨迹更新OD，得到人群轨迹
   * @param imsi 用户IMSI
   * @param oWhere 用户轨迹的O位置节点，与O位置相同或者属于O位置
   * @param dWhere 用户轨迹的D位置节点，与D位置相同或者属于D位置
   */
  def updateOd(imsi: String, oWhere: Where, dWhere: Where): Unit ={
    //更新人群和行为数据
    val user = new User()
    user.imsi = imsi
    crowd.add(user)
    val oTmp = new Location
    oTmp.lLA = oWhere.lLA
    oTmp.code = oWhere.code
    oTmp.crowd = crowd
    oLoc = oTmp
    val dTmp = new Location
    dTmp.lLA = dWhere.lLA
    dTmp.code = dWhere.code
    dTmp.crowd = crowd
    dLoc = dTmp
    val uTime = (dWhere.sTime - oWhere.eTime).toInt
    val uDistance = dWhere.getDistance(oWhere)
    val uMhDistance = dWhere.getMhDistance(oWhere)
//    println(imsi)
//    println(uTime)
//    println(uDistance)
//    println(uMhDistance)
    behaviors.put(imsi, (uTime, uDistance,uMhDistance))
    //更新时间和距离
    if(crowd.headcount() > 1) {
      time = (uTime + (crowd.headcount()-1) * time) / crowd.headcount()
      distance = (uDistance + (crowd.headcount()-1) * distance) / crowd.headcount()
      mHDistance = (uMhDistance + (crowd.headcount()-1) * mHDistance) / crowd.headcount()
    }
    else {
      time = (dWhere.sTime - oWhere.eTime).toInt
      distance = dWhere.getDistance(oWhere)
      mHDistance = dWhere.getMhDistance(oWhere)
    }
  }
  /**
   * 提取O地到D地的轨迹，可能是多次的轨迹
   * @param trace: 用户轨迹轨迹
   * @return O地到D地的轨迹
   */
  def getOdTraces(trace: Trace): ListBuffer[Trace] ={
    val traces: ListBuffer[Trace] = trace.getSubTraces(oLoc.code, dLoc.code)
//    println(traces)
    traces
  }

  /**
   * 根据多个traces生成odWheres,可能来自于多个用户或者一个用户
   * @param traces  多个traces
   * @return odWheres
   */
  def getOdWheres(traces: mutable.HashMap[String, Trace]): List[(String, Where, Where)] = {
    val odWheres = new ListBuffer[(String, Where, Where)]
    for (trace <- traces) {
//      println(com.bonc.trace)
      val odWheres_in: List[(String, Where, Where)] = getOdWheres(trace._2)
//      println(odWheres)
      for (i <- odWheres_in){
//        println(i)
        odWheres.append(i)
      }
    }
//    println(odWheres)
    odWheres.toList
  }
  /**
   * 从某用户轨迹中提取odWhere对
   * @param trace 用户轨迹
   * @return odWhere对，可能是多个
   */
  def getOdWheres(trace: Trace): List[(String, Where, Where)] ={
    val odWheres = new ListBuffer[(String, Where, Where)]
    for(trace <- getOdTraces(trace)){
//      println(com.bonc.trace)
      odWheres.append((trace.imsi, trace.wheres.head,trace.wheres.last))
    }
//    println(odWheres)
    odWheres.toList
  }

}
