package com.bonc.location

import com.bonc.lla.LLA
import mrLocateV2.bsparam.Cell
import mrLocateV2.coordinate.CoordinateLB

import scala.collection.immutable.HashSet

/**
  * 由一系列的基站扇区代表一个位置区域
  */
class Sectors extends Serializable {
  var ecis: HashSet[Long] = _

  /**
    * 为扇区集合添加新元素
    * @param eci 新的扇区
    */
  def addEci(eci: Long): Unit ={
    ecis += eci
  }

  /**
    * 判断位置是否包含指定位置
    * @param sector 指定位置
    */
  def contains(sector: Option[Sector]): Boolean ={
    sector match{
      case Some(x) => ecis.contains(x.eci)
      case None => false
    }
  }

  /**
   * 获取所有距离输入点的距离小于输入距离的所有eci
   * @param lla 自定义的经纬度坐标
   * @param radius  半径
   * @param cellMap 全部的扇形小区
   * @return
   */
  def allInRoundECIS(lla: LLA, radius:Int, cellMap:java.util.HashMap[java.lang.Long, Cell]):Unit ={

    import scala.collection.JavaConversions._
    for (i <- cellMap){
      val lB: CoordinateLB = i._2.getLB
      val cellCenterPoint = new LLA(lB.getLongtitude, lB.getLatitude)
      if (lla.getDistance(cellCenterPoint) < radius){
        this.addEci(i._1)
      }
    }
  }

}





















