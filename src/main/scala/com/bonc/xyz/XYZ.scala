package com.bonc.xyz

import com.bonc.lla.LLA

import scala.collection.mutable

/**
  * //笛卡尔三个坐标轴
  *
  * @param x //X轴坐标（沿赤道向东方向）
  * @param y //轴坐标（沿子午线方向向北）
  */
class XYZ(val x: Double = Double.NaN, val y: Double = Double.NaN) extends Serializable {
  var z  = Double.NaN //默认为NAN
   //考虑海拔高度
  def this(x: Double, y: Double, z: Double) {
    this(x, y)
    this.z = z
  }

  override def toString: String = {
    val sb: StringBuilder = new mutable.StringBuilder()
    sb.append(if(!x.isNaN)  x else "")
    sb.append(",")
    sb.append(if(!y.isNaN)  y else "")
    sb.append(",")
    sb.append(if(!z.isNaN)  z else "")
    sb.toString()
  }

  /**
    * 求两点之间的欧几里得距离
    *
    * @param xYZ 另一点坐标
    * @return 欧几里得距离
    */
  def getDistance(xYZ: XYZ): Double = {
    val d = Math.sqrt(Math.pow(xYZ.x - this.x, 2) + Math.pow(xYZ.y - this.y, 2))
    d
  }
  /**
    * 求两点之间的曼哈顿距离
    *
    * @param xYZ 另一点坐标
    * @return 曼哈顿距离
    */
  def getMhDistance(xYZ: XYZ): Double = {
    val d = Math.abs(xYZ.x - this.x) + Math.abs(xYZ.y - this.y)
    d
  }
  /**
    * 由高斯投影坐标转换成经纬度坐标，采用使用WGS84坐标系
     */
  def toLLA(L0: Double): LLA = { //存放转换后的LB坐标
    var ProjNo = 0
    var ZoneWide = 0
    var longitude1 = .0
    var latitude1 = .0
    var longitude0 = .0
    var X0 = .0
    var Y0 = .0
    var xval = .0
    var yval = .0
    var e1 = .0
    var e2 = .0
    var f = .0
    var a = .0
    var ee = .0
    var NN = .0
    var T = .0
    var C = .0
    var M = .0
    var D = .0
    var R = .0
    var u = .0
    var fai = .0
    var iPI = .0
    iPI = 0.0174532925199433333333
    a = 6378137
    f = 1 / 298.257223563
    ZoneWide = 6
    ProjNo = (this.x / 1000000L).toInt // 查找带号

    ProjNo = (L0 + 3).toInt / ZoneWide
    longitude0 = (ProjNo - 1) * ZoneWide + ZoneWide / 2
    longitude0 = longitude0 * iPI
    val X1 = this.x + (ProjNo - 1) * 1000000 //+500000;
    X0 = (ProjNo - 1) * 1000000L + 500000L
    Y0 = 0
    xval = X1 - X0
    yval = this.y - Y0
    e2 = 2 * f - f * f
    e1 = (1.0 - Math.sqrt(1 - e2)) / (1.0 + Math.sqrt(1 - e2))
    ee = e2 / (1 - e2)
    M = yval
    u = M / (a * (1 - e2 / 4 - 3 * e2 * e2 / 64 - 5 * e2 * e2 * e2 / 256))
    fai = u + (3 * e1 / 2 - 27 * e1 * e1 * e1 / 32) * Math.sin(2 * u) + (21 * e1 * e1 / 16 - 55 * e1 * e1 * e1 * e1 / 32) * Math.sin(4 * u) + (151 * e1 * e1 * e1 / 96) * Math.sin(6 * u) + (1097 * e1 * e1 * e1 * e1 / 512) * Math.sin(8 * u)
    C = ee * Math.cos(fai) * Math.cos(fai)
    T = Math.tan(fai) * Math.tan(fai)
    NN = a / Math.sqrt(1.0 - e2 * Math.sin(fai) * Math.sin(fai))
    R = a * (1 - e2) / Math.sqrt((1 - e2 * Math.sin(fai) * Math.sin(fai)) * (1 - e2 * Math.sin(fai) * Math.sin(fai)) * (1 - e2 * Math.sin(fai) * Math.sin(fai)))
    D = xval / NN
    longitude1 = longitude0 + (D - (1 + 2 * T + C) * D * D * D / 6 + (5 - 2 * C + 28 * T - 3 * C * C + 8 * ee + 24 * T * T) * D * D * D * D * D / 120) / Math.cos(fai)
    latitude1 = fai - (NN * Math.tan(fai) / R) * (D * D / 2 - (5 + 3 * T + 10 * C - 4 * C * C - 9 * ee) * D * D * D * D / 24 + (61 + 90 * T + 298 * C + 45 * T * T - 256 * ee - 3 * C * C) * D * D * D * D * D * D / 720)
    //返回LLA
    new LLA(longitude1 / iPI, latitude1 / iPI, z)
  }
}

object XYZ{
  def main(args: Array[String]): Unit = {
    val gaussXYDeal: XYZ = new XYZ(284412.6898810635, 4218172.101956899)
  //  gaussXYDeal.toLLA(117)
    val lb = gaussXYDeal.toLLA(117)
   // println(lb.latitude + "   " + lb.)
    val xx = lb.toXYZ(117)
    println(xx.x + "   " + xx.y)
  }
}