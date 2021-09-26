package com.bonc.trace

import java.util

import com.bonc.lla.LLA
import com.bonc.xyz.XYZ
import com.bonc.data.{Record, Scene}
import com.bonc.data.Scene.Scene
import com.bonc.trace.WhereType.{PASSBY, WhereType}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * 定位用户位置
  */
class Where extends Comparable[Where] with Serializable {
  var lLA:LLA = _ //用户坐标
  var code:String = _ //位置编码，可以是栅格中心坐标、小区ECI和AOI编号，通过编号可以推断是什么类型的位置
  var sTime: Long = 0l //在该位置的起始时间，默认为0
  var eTime: Long = 0l //在该位置的结束时间,默认为0
  var duration: Int = 0 //用户在该位置的时间 eTime - sTime
  var scene: Scene = _ //场景，如果生成scene数据场景存在INDOOR则为INDOOR
  var wType: WhereType = _ //WHERE的类型，居住地、工作地或经留点等
  var lingerTimeThr = 60 //分钟，驻留时间门限，大于该时间认为是驻留该地点（时间太短说明只是过境）
  var ecis = new ListBuffer[Long]
  var rat = 0
  var source:String = ""
  var provinceCity:String = ""

  def this(lLA:LLA, code:String){
    this()
    this.lLA = lLA
    this.code = code
  }

  override def toString: String = {
    val sb: StringBuilder = new mutable.StringBuilder()
    sb.append(if(lLA != null) lLA.toString else ","+",")//都是空
    sb.append(",")
    sb.append(if(code != null) code else "")
    sb.append(",")
    sb.append(if(sTime != 0) sTime else "")
    sb.append(",")
    sb.append(if(eTime!= 0) eTime else "")
    sb.append(",")
    sb.append(duration)
    sb.append(",")
    if(scene == Scene.INDOOR)   sb.append( "INDOOR" )
    else                        sb.append( "ROAD" )
    sb.toString()
  }

  /**
    * 计算得到两个位置的曼哈顿距离
    * @param location 结束位置
    * @return 曼哈顿距离
    */
  def getMhDistance(location: Where): Double ={
    lLA.getMhDistance(location.lLA)
  }
  /**
    * 计算得到两个位置的欧几里得距离
    * @param location 结束位置
    * @return 欧几里得距离
    */
  def getDistance(location: Where): Double ={
    lLA.getDistance(location.lLA)
  }
  /**
    * 生成用户位置及相关描述信息,如果只有一个消息时，sTime = eTime, duration = 0
    * @param records 定位记录字典，如果是小区/栅格/AOI轨迹，则为连续归属同一小区的RECORDS（按时间顺序排列）；    *
    * @param code location标识,可以是Eci/gridCode/aoaCode
    */
  def create(records: List[Record], code: String): Unit = {
    //赋值起始和结束时间，只考虑record的起始时间 ==》 只考虑起始时间的话，只有一段的无法计算
    sTime = records.head.sTime
    eTime = records.last.sTime
    duration = (eTime - sTime).toInt
    //保存location编码
    this.code = code
    //求解平均用户坐标
    val lLAs = new ListBuffer[LLA]()
    records.foreach(record=>{
      lLAs.append(record.lLA)
      //如果record场景为INDOOR,则为INDOOR
      if(scene!= Scene.INDOOR) scene = record.scene
    })
    lLA = new LLA().getAvgLLA(lLAs.toList)
    rat = records.head.rat
    val buffer = new StringBuffer("|")
    val strings = new ListBuffer[String]
    records.foreach(y => {
      strings.append(y.locateType.toString)
    })
    strings.distinct.foreach(y => {
      buffer.append(y)
    })
    source = buffer.toString
  }

  /**
    * 根据sTime对location进行排序，这样保证trace中的locations是按照时间排序的
    * @param o 其它的location
    * @return 比较结果
    */
  override def compareTo(o: Where): Int = (this.sTime - o.sTime).toInt

  /**
    * 更新AOI位置区域* @param records 定位记录字典，为连续归属同一服务而且为SpaceType==INDOOR场景的RECORDS（该方法作为备用）
    * @param avgLLA records的平均坐标（之前已经做好计算）
    * @param code 位置编码
    */
  def create(records: List[Record], avgLLA: LLA, code: String): Unit = {
    //赋值起始和结束时间，只考虑record的起始时间
    sTime = records.head.sTime
    eTime = records.last.sTime
    duration = (eTime - sTime).toInt
    //求解平均用户坐标
    lLA = avgLLA
    records.foreach(record=>{
      //如果record场景为INDOOR,则为INDOOR
      if(scene != Scene.INDOOR) scene = record.scene
    })
    //生成location编码
    this.code = code
  }

  /**
    * 合并两个位置对象
    * @param location 待合并位置
    */
  def merge(location: Where): Unit ={
    //如果两个位置处于同一个位置，则合并
    if(!code.equals(location.code)) {
      throw new IllegalArgumentException("输入location的code与当前不一致")
    }
    //时间合并
    if(sTime > location.sTime)  sTime = location.sTime
    if(eTime < location.eTime)  eTime = location.eTime
    duration = (eTime - sTime).toInt
    //位置做平均
    if (location.lLA.latitude != Double.NaN && location.lLA.longitude != Double.NaN) {
      lLA.average(location.lLA)
    }
    //如果record场景为INDOOR,则为INDOOR
    if(scene != Scene.INDOOR) scene = location.scene
  }

}

object Where {
  def main(args: Array[String]): Unit = {
   val rec1 = new Record()
    rec1.sTime = 1000l
    rec1.lLA = new XYZ(100, 200).toLLA(LLA.L0)

    val rec2 = new Record()
    rec2.sTime = 2000l
    rec2.lLA = new XYZ(300, 400).toLLA(LLA.L0)
    val recs = new ListBuffer[Record]
    recs.append(rec1)
    recs.append(rec2)

    val loc1 = new Where()
    loc1.lLA = new LLA(112, 34)
    loc1.sTime = 1000l
    loc1.eTime = 3000l
    loc1.code = "1111"
    loc1.scene = Scene.INDOOR
    val loc2 = new Where()
    loc2.lLA = new LLA(111, 30)
    loc2.sTime = 2000l
    loc2.eTime = 5000l
    loc2.code = "1111"
    loc2.scene = Scene.ROAD
    loc2.merge(loc1)
    println(loc2.toString)

   // println(ls.head.sTime)
  }

}