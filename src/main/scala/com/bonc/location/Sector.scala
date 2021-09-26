package com.bonc.location

import com.bonc.lla.LLA

/**
 * 扇区，主要用户小区定位情况
 */
class Sector extends Location with Serializable {
  var eci: Long = _ //小区eci

  /**
   * 计算小区面积
   * @return 小区面积
   */
  override def area(): Double ={
    300 //默认是300平方米
  }

  /**
   * 一个小区是否包含另外一个小区
   * @param otherSector 小区
   * @return true/false
   */
  def contains(otherSector: Sector): Boolean ={
    equals(otherSector)
  }

  override def contains(lLA: LLA): Boolean = ???

  override def containsLoc(otherLoc: Location): Boolean = ???
}