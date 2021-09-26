package com.bonc.location

import com.bonc.lla.LLA
import com.bonc.xyz.XYZ

/**
  * 栅格的键值，由直角坐标和栅格尺度生成
  */
class GridKey() extends Serializable {
  var X: Int = Int.MinValue  //X坐标整型，GRID中心坐标取整
  var Y: Int = Int.MinValue //Y坐标整型，GRID中心坐标取整
  var Z: Int = Int.MinValue//Z坐标整型，GRID中心坐标取整
  var size: Int = 50 //GRID尺度，默认为50m

  def this(X: Int, Y: Int){
    this()
    this.X = X
    this.Y = Y
  }

  def this(X: Int, Y: Int, Z: Int){
    this()
    this.X = X
    this.Y = Y
    this.Z = Z
  }
  def this(lla: LLA,L0:Double){
    this()
    val xyz: XYZ = lla.toXYZ(L0).asInstanceOf[XYZ]
    this.X = xyz.x.toInt
    this.Y = xyz.y.toInt
    this.Z = xyz.z.toInt
  }
  override def toString(): String = {
    val sb: StringBuilder  = new StringBuilder
    sb.append(if(X == Int.MinValue) ""else X )
    sb.append(",")
    sb.append(if(Y == Int.MinValue) "" else Y)
    sb.append(",")
    sb.append(if(Z == Int.MinValue) "" else Z)
    sb.append(",")
    sb.append(size)
    sb.toString()
  }
  override def hashCode: Int = (X, Y, Z, size).##

  override def equals(o: Any): Boolean = {
    if (!o.isInstanceOf[GridKey]) return false
    val key = o.asInstanceOf[GridKey]
    (this.X == key.X) && (Y == key.Y) && (Z == key.Z) && (size == key.size)
  }


  /**
    * 根据输入的经纬度坐标，在默认栅格尺度下，生成gridKey
    * @param lLA 经度纬度，海拔高度
    * @param refL0 参考经度
    * @return 栅格键值
    */
  def create(lLA: LLA, refL0: Double): GridKey = {
    if(lLA != null) {
      val xYZ: XYZ = lLA.toXYZ(refL0).asInstanceOf[XYZ]
      X = (xYZ.x / size).toInt
      Y = (xYZ.y / size).toInt
      Z = (xYZ.z / size).toInt
    }
    this
  }
  /**
    * 根据输入的经纬度坐标和指定栅格尺度，生成gridKey
    * @param lLA 经度纬度，海拔高度
    * @param refL0 参考经度
    * @param insize 栅格尺度
    * @return 栅格键值
    */
  def create(lLA: LLA, refL0: Double, insize: Int): GridKey = {
    size = insize
    if(lLA != null) {
      val xYZ: XYZ = lLA.toXYZ(refL0).asInstanceOf[XYZ]
      X = (xYZ.x / size).toInt
      Y = (xYZ.y / size).toInt
      Z = (xYZ.z / size).toInt
    }
    this
  }
  /**
    * 生成Grid的code
    * @return code
    */
  def makeCode(): String ={
    val sb: StringBuilder  = new StringBuilder
    sb.append(X)
    sb.append("~")
    sb.append(Y)
    sb.append("~")
    sb.append(Z)
    sb.append("~")
    sb.append(size)
    sb.toString()
  }

  /**
    * 中心点转换成LLA
    * @return LLA
    */
  def centerLLA(): LLA ={
    new XYZ(X,Y, Z).toLLA(LLA.L0)
  }

}



object GridKey{
  def main(args: Array[String]): Unit = {
    val my1: GridKey = new GridKey()
    my1.create(new LLA(112, 34), 110)
//    my1.X =100
//    my1.Y = 200
//    my1.Z = 300
//    my1.size = 50
    val my2: GridKey = new GridKey()
    my2.X =100
    my2.Y = 200
    my2.Z = 300
    my2.size = 50

    println(my2.makeCode())
  }

}