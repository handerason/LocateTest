package com.bonc.data

import java.util

import com.bonc.lla.LLA
import com.bonc.location.{GridKey, LocType}
import com.bonc.location.LocType.LocType
import com.bonc.data.LocateType.LocateType
import com.bonc.data.Scene.Scene

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * 定位记录，从定位结果数据库中的每个信息定位结果提取相关字段生成
  * 定位记录如果来自于AOI定位，则需要每个消息定位结果都有AOI编号的标识；如果是非AOI定位，则gridCode
  * 可以在生成Record时使用GridKey.create(LLA)生成。定位类型，需要根据实际情况输入；用户场景和会话标识，
  * 需要在融合定位结果中增加
  */
class Record extends Comparable[Record] with Serializable {
  var lLA: LLA = _ //经纬度坐标
  var imsi: String = _ //用户IMSI
  var sTime: Long = 0 //记录开始时间,默认为0
  var eTime: Long = 0 //记录结束时间,默认为0
  var scene: Scene = _ //用户场景
  var session: Int = (Math.random() * 10000000).toInt //会话标识 取值为Int范围内任意值，随机产生，区别不同的会话
  var rat:Int = 0
  var eci: Long = _ //所在网络小区id
  var gridCode: String = _ //所在Grid编号
  var aoiCode: String = _ //所在aoi编号
  var locateType: LocateType = _ //定位类型

  override def toString: String = {
    val sb: StringBuilder = new mutable.StringBuilder()
    sb.append(if (lLA != null) lLA.toString else "," + ",") //都是空
    sb.append(",")
    sb.append(if (imsi != null) imsi else "")
    sb.append(",")
    sb.append(if (sTime != 0) sTime else "")
    sb.append(",")
    sb.append(if (eTime != 0) eTime else "")
    sb.append(",")
    sb.append(if (scene != null) scene else "")
    sb.append(",")
    sb.append(session)
    sb.append(",")
    sb.append(if (eci != 0) eci else "")
    sb.append(",")
    sb.append(if (gridCode != null) gridCode else "")
    sb.append(",")
    sb.append(if (aoiCode != null) aoiCode else "")
    sb.append(",")
    sb.append(if (locateType != null) locateType else "")
    sb.toString()
  }

  /**
    * 生成Session编号，随机生成
    */
  def generateSession(): Int = {
    (Math.random() * 10000000).toInt
  }

  /**
    * 根据轨迹类型选择定位类型（默认类型为CELL定位）
    *
    * @param traceType 轨迹类型
    * @return 定位类型
    */
  def getCode(traceType: LocType): String = {
    if (traceType == LocType.AOI) aoiCode
    else if (traceType == LocType.GRID) gridCode
    else eci.toString
  }

  /**
    * 根据sTime对Record进行排序，这样保证trace中的locations是按照时间排序的
    *
    * @param o 其它的Record
    * @return 比较结果
    */
  override def compareTo(o: Record): Int = (this.sTime - o.sTime).toInt

  /**
    * 根据LLA坐标生成栅格编码GridCode
    */
  def generateGridCode(): Unit = {
    val gridKey: GridKey = new GridKey
    gridKey.create(lLA, LLA.L0) //定位到栅格
    gridCode = gridKey.makeCode() //并回填gridCode
  }

  /**
    * 根据LLA坐标生成栅格编码GridCode
    * param size 指定栅格尺寸
    */
  def generateGridCode(size: Int): Unit = {
    val gridKey: GridKey = new GridKey
    gridKey.create(lLA, LLA.L0, size) //定位到栅格
    gridCode = gridKey.makeCode() //并回填gridCode
  }

  /**
    * 对单个定位结果进行解析生成Record，需要根据具体格式撰写
    */
  def parse(locResult: String): Unit = {
    val strs = locResult.split(",")
    imsi = strs(0)
    sTime = strs(1).toLong
    eTime = strs(2).toLong
    eci = strs(4).toLong
    scene = Scene.stringTo(strs(6))
    session = strs(7).toInt
    lLA = new LLA(strs(8).toDouble, strs(9).toDouble)
  }
  /**
   * 对多个定位结果进行解析生成Record，需要根据具体格式撰写
   */
  def parse2Records(locResStrs: List[String]): List[Record] = {
    //保存结果，按时间顺序
    val records = new ListBuffer[Record]
    //遍历，解析，保存
    for (locResStr <- locResStrs) {
      val record = new Record
      record.parse(locResStr)
      records.append(record)
    }
    //先按时间排序，然后再按eci排序
    records.toList.sortBy(record => record.sTime)
  }

  /**
   * 对XDR定位结果（含XDR全部信息）进行解析生成Records，需要根据具体格式撰写
   * 本方法对目标小区也生成独立的Record，这样的好处是对于一个小区的进入时间能够有更准确的描述
   * 根据两个小区间的切出和切入时间可判断是否为切换事件发生
   */
  def parse2Records(locResStr: String): List[Record] = {
    val records = new ListBuffer[Record]() //保存结果
    val strs = locResStr.split(",")
    val record = new Record()
    record.parse(locResStr)
    records.append(record)
    if (!strs(5).equals("4294967295")) {
      record.imsi = strs(0)
      record.sTime = strs(2).toLong
      record.eTime = strs(2).toLong
      record.eci = strs(5).toLong
      record.scene = Scene.stringTo(strs(6))
      record.session = strs(7).toInt
      // record.lLA = new LLA(strs(8).toDouble, strs(9).toDouble)//坐标需要从工参中查询到
      records.append(record)
    }
    records.toList
  }
  /**
   * 输入信息定位结果的接口方法，根据输入的类型可生成多个code
   * 按照用户的一次Session为单位，提取字段的过程,生成records，并生成需要的编码
   *
   * @param records 未打gridCode和AoiCode的records
   */
  def generateCodes(records: List[Record], traceTypes: LocType, hmmURL:String): Unit = {
    //如果包含GRID轨迹类型，则生成GridCode
    if (traceTypes.equals(LocType.GRID)) {
      generateGridCode(records)
    }
    //如果包含AOI轨迹类型，则生成aoiCode
    if (traceTypes.equals(LocType.AOI)) {
//      generateAoiCode(records,hmmURL)
    }
  }
  /**
   * 对一个Scession的records生成AOI位置编码aoiode
   *
   * @param records 为1某用户1天的数据：1
   */
//  def generateAoiCode(records: List[Record], hmmURL:String): Unit = {
//    var aoi = ""
//    if (records.nonEmpty) {
//      if (records.head.scene == Scene.INDOOR) { //仅仅处理室内场景records
//        if (records.head.scene == Scene.INDOOR) { //如果是室内场景，则对平均值处理，生成总gridCode
//          //求解平均用户坐标
//          val lLAs = new ListBuffer[LLA]()
//          records.foreach(record => {
//            lLAs.append(record.lLA)
//          })
//          val avgLLA: LLA = new LLA().getAvgLLA(lLAs.toList)
//          //定位到AOI，获取AOI编码
//          val pointSolr = new pointMappingSurface
//          val communities: util.List[Community] = pointSolr.find_CommunityIds(hmmURL, avgLLA.longitude.toString, avgLLA.latitude.toString)
//          if (!communities.isEmpty){
//            aoi = communities.get(0).getCommunityId
//          }else{
//            aoi = ""
//          }
//          records.foreach(x => x.aoiCode = aoi)
//        }
//      }
//    }
//
//  }
  /**
   * 对一个Scession的records生成栅格编码gridCode(使用默认栅格尺度)
   *
   * @param records 为1某用户1天的数据：1
   */
  def generateGridCode(records: List[Record]): Unit = {
    if (records.nonEmpty) {
      if (records.head.scene == Scene.INDOOR) { //如果是室内场景，则对平均值处理，生成总gridCode
        //求解平均用户坐标
        val lLAs = new ListBuffer[LLA]()
        records.foreach(record => {
          lLAs.append(record.lLA)
        })
        val avgLLA: LLA = new LLA().getAvgLLA(lLAs.toList)
        //定位到栅格并回填gridCode
        val gridKey: GridKey = new GridKey
        gridKey.create(avgLLA, LLA.L0)
        val gridCode = gridKey.makeCode()
        records.foreach(record => {
          record.gridCode = gridCode
        })
      }
      else { //如果是室外场景，则逐条record分别生成gridCode
        records.foreach(record => {
          record.generateGridCode()
        }) //如果是室内场景，则对平均值处理，生成总gridCode
      }
    }
  }
  /**
   * 对一个Scession的records生成栅格编码gridCode(使用指定栅格尺度)
   *
   * @param records 为1某用户1天的数据：1
   */
  def generateGridCode(records: List[Record], size: Int): Unit = {
    if (records.nonEmpty) {
      if (records.head.scene == Scene.INDOOR) { //如果是室内场景，则对平均值处理，生成总gridCode
        //求解平均用户坐标
        val lLAs = new ListBuffer[LLA]()
        records.foreach(record => {
          lLAs.append(record.lLA)
        })
        val avgLLA: LLA = new LLA().getAvgLLA(lLAs.toList)
        //定位到栅格并回填gridCode
        val gridKey: GridKey = new GridKey
        gridKey.create(avgLLA, LLA.L0, size)
        val gridCode = gridKey.makeCode()
        records.foreach(record => {
          record.gridCode = gridCode
        })
      }
      else { //如果是室外场景，则逐条record分别生成gridCode
        records.foreach(record => {
          record.generateGridCode(size)
        }) //如果是室内场景，则对平均值处理，生成总gridCode
      }
    }
  }
  /**
   * 根据输入的定位结果字符串，生成records带位置编号
   * @param locResStrs 定位结果字符串集合
   * @param traceTypes 轨迹类型
   * @return records带位置编号
   */
  def createRecords(locResStrs: List[String], traceTypes: LocType,hmmURL:String): List[Record] ={
    //解析，生成records
    val records = parse2Records(locResStrs: List[String])
    //如果包含GRID轨迹类型，则生成GridCode
    if (traceTypes.equals(LocType.GRID)) {
      generateGridCode(records)
    }
    //如果包含AOI轨迹类型，则生成aoiCode
    if (traceTypes.equals(LocType.AOI)) {
      //generateAoiCode(records,hmmURL)//需要重写
    }
    records
  }
}

object Record {
  def main(args: Array[String]): Unit = {
    val record = new Record
    record.lLA = new LLA(10,20)
    record.eci = 10000l
    record.gridCode = "gridcode"
    record.aoiCode = "aoaCode"
    record.imsi = "imsi"
    record.session = (Math.random()*10000).toInt
    println(record.getCode(LocType.GRID))
    record.generateGridCode(20)
    println(record.gridCode)

    val record4 = new Record
    record4.parse("imsi4,1599622711000,1599623311000,,50000,,INDOOR,3695368,15,25")
//    println(record4.toString)

    val strings = new ListBuffer[String]()
    strings.append("imsi4,1599622711000,1599623311000,,50000,,INDOOR,3695368,15,25")
    strings.append("imsi5,1599623711000,1599623312000,,60000,,INDOOR,3695362,16,26")
    strings.append("imsi6,1599624711000,1599623313000,,70000,,INDOOR,3695365,17,27")
    strings.append("imsi7,1599625711000,1599623314000,,80000,,INDOOR,3695367,18,28")
    val records: List[Record] = record4.parse2Records(strings.toList)

    record4.generateGridCode(records,20)
    for (i <- records){
      println(i.gridCode)
    }


    val record1 = new Record
    record1.lLA = new LLA(11,21)
    record1.eci = 20000l
    record1.gridCode = "gridcode1"
    record1.aoiCode = "aoaCode1"
    record1.imsi = "imsi1"
    record1.session = (Math.random()*10000).toInt

    val record2 = new Record
    record2.lLA = new LLA(12,22)
    record2.eci = 30000l
    record2.gridCode = "gridcode2"
    record2.aoiCode = "aoaCode2"
    record2.imsi = "imsi2"
    record2.session = (Math.random()*10000).toInt

    val record3 = new Record
    record3.lLA = new LLA(13,23)
    record3.eci = 40000l
    record3.gridCode = "gridcode3"
    record3.aoiCode = "aoaCode3"
    record3.imsi = "imsi3"
    record3.session = (Math.random()*10000).toInt


  }
}





















