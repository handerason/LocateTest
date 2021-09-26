package com.bonc.lla

import com.bonc.xyz.XYZ

import scala.collection.mutable

/**
  * WGS84大地坐标系
  *
  * @param longitude 经度
  * @param latitude 纬度
  */
class LLA(var longitude: Double = Double.NaN, var latitude: Double = Double.NaN) extends Serializable {
  var altitude: Double = Double.NaN //默认为非数字

  def this(longitude:Double, latitude:Double, altitude:Double){
    this(longitude, latitude)
    this.altitude = altitude
  }

  override def toString: String = {
    val sb: StringBuilder = new mutable.StringBuilder()
    sb.append(if(!longitude.isNaN)  longitude else "")
    sb.append(",")
    sb.append(if(!latitude.isNaN)  latitude else "")
    sb.append(",")
    sb.append(if(!altitude.isNaN)  altitude else "")
    sb.toString()
  }

  /**
    * 求两点之间的欧几里得距离
    *
    * @param lLA 另一点坐标
    * @return 欧几里得距离
    */
  def getDistance(lLA: LLA): Double = {
    if(lLA != null) lLA.toXYZ(LLA.L0).getDistance(new LLA(longitude, latitude).toXYZ(LLA.L0))
    else            Double.MaxValue
  }
  /**
    * 求两点之间的曼哈顿距离
    *
    * @param lLA 另一点坐标,如果lLA为null则距离穷大
    * @return 曼哈顿距离
    */
  def getMhDistance(lLA: LLA): Double = {
    if(lLA != null) lLA.toXYZ(LLA.L0).getMhDistance(new LLA(longitude, latitude).toXYZ(LLA.L0))
    else            Double.MaxValue
  }
  /**
    * 平均坐标
    * @param lLA
    */
  def average(lLA: LLA): Unit = {
    if(lLA != null) {
      val xYZ = lLA.toXYZ(LLA.L0)
      val thisXYZ = this.toXYZ(LLA.L0)
      val sumX = thisXYZ.x + xYZ.x
      val sumY = thisXYZ.y + xYZ.y
      val sumZ = thisXYZ.z + xYZ.z
      //求平均LLA
      val thisLLA = new XYZ(sumX / 2, sumY / 2, sumZ / 2).toLLA(LLA.L0)
      //赋值
      longitude = thisLLA.longitude
      latitude = thisLLA.latitude
      altitude = thisLLA.altitude
    }
  }
  /**
    * 大地坐标系转换为笛卡尔坐标系
     * @param refL0       参考中心经度
    */
  def toXYZ(refL0: Double): XYZ = {
    var B = .0
    var L = .0
    var l = .0
    var t = .0
    var m = .0
    var N = .0
    var q2 = .0
    var x = .0
    var y = .0
    var s = .0
    var f = .0
    var e2 = .0
    var a = .0
    var a1 = .0
    var a2 = .0
    var a3 = .0
    var a4 = .0
    var b1 = .0
    var b2 = .0
    var b3 = .0
    var b4 = .0
    var c0 = .0
    var c1 = .0
    var c2 = .0
    var c3 = .0
    val IPI = 0.0174532925199433333333
    B = latitude
    L = longitude
    a = 6378137
    f = 1 / 298.257223563
    val L0 = refL0 * IPI
    L = L * IPI
    B = B * IPI
    e2 = 2 * f - f * f
    l = L - L0
    t = Math.tan(B)
    m = l * Math.cos(B)
    N = a / Math.sqrt(1 - e2 * Math.sin(B) * Math.sin(B))
    q2 = e2 / (1 - e2) * Math.cos(B) * Math.cos(B)
    a1 = 1 + 3.toDouble / 4 * e2 + 45.toDouble / 64 * e2 * e2 + 175.toDouble / 256 * e2 * e2 * e2 + 11025.toDouble / 16384 * e2 * e2 * e2 * e2 + 43659.toDouble / 65536 * e2 * e2 * e2 * e2 * e2
    a2 = 3.toDouble / 4 * e2 + 15.toDouble / 16 * e2 * e2 + 525.toDouble / 512 * e2 * e2 * e2 + 2205.toDouble / 2048 * e2 * e2 * e2 * e2 + 72765.toDouble / 65536 * e2 * e2 * e2 * e2 * e2
    a3 = 15.toDouble / 64 * e2 * e2 + 105.toDouble / 256 * e2 * e2 * e2 + 2205.toDouble / 4096 * e2 * e2 * e2 * e2 + 10359.toDouble / 16384 * e2 * e2 * e2 * e2 * e2
    a4 = 35.toDouble / 512 * e2 * e2 * e2 + 315.toDouble / 2048 * e2 * e2 * e2 * e2 + 31185.toDouble / 13072 * e2 * e2 * e2 * e2 * e2
    b1 = a1 * a * (1 - e2)
    b2 = -1.toDouble / 2 * a2 * a * (1 - e2)
    b3 = 1.toDouble / 4 * a3 * a * (1 - e2)
    b4 = -1.toDouble / 6 * a4 * a * (1 - e2)
    c0 = b1
    c1 = 2 * b2 + 4 * b3 + 6 * b4
    c2 = -(8 * b3 + 32 * b4)
    c3 = 32 * b4
    s = c0 * B + Math.cos(B) * (c1 * Math.sin(B) + c2 * Math.sin(B) * Math.sin(B) * Math.sin(B) + c3 * Math.sin(B) * Math.sin(B) * Math.sin(B) * Math.sin(B) * Math.sin(B))
    y = s + 1.toDouble / 2 * N * t * m * m + 1.toDouble / 24 * (5 - t * t + 9 * q2 + 4 * q2 * q2) * N * t * m * m * m * m + 1.toDouble / 720 * (61 - 58 * t * t + t * t * t * t) * N * t * m * m * m * m * m * m
    x = N * m + 1.toDouble / 6 * (1 - t * t + q2) * N * m * m * m + 1.toDouble / 120 * (5 - 18 * t * t + t * t * t * t - 14 * q2 - 58 * q2 * t * t) * N * m * m * m * m * m
    x = x + 500000
    new XYZ(x, y, altitude)
  }

  /**
   * 平均坐标
   * @param lLAs LLA集合
   * @return 平均LLA
   */
  def getAvgLLA(lLAs: List[LLA]): LLA ={
    var sumX, sumY, sumZ: Double = 0d
    var num: Int = 0
    for(lLA <- lLAs){
      if(lLA != null) {
        val xYZ: XYZ = lLA.toXYZ(LLA.L0)
        sumX += xYZ.x
        sumY += xYZ.y
        sumZ += xYZ.z
        //计数
        num += 1
      }
    }
    if(num > 0) new XYZ(sumX/num,sumY/num, sumZ/num).toLLA(LLA.L0)
    else        null
  }

  /**
   * 根据输入纬度和带宽返回参考中心纬度
   * @param longitude 纬度
   * @return 中心经度L0
   */
  def getL0(longitude: Double): Double = {
    val prjno = (longitude / 6).toInt + 1 //投影带号
    prjno * 6 - 3
  }
}

object LLA{
  //参考中央经线度数
  val L0 = 105 //tianyuan
  //public static double L0=93.0;//gansu
  //public static double L0=117;//tianjin
  def main(args: Array[String]): Unit = {
    val lLA1 = new LLA(112, 42.0)
    val lLA2 = new LLA(114, 42.0)
    //println(new XYZ(2,0))
    lLA1.average(lLA2)
    println(lLA1)

  }
}