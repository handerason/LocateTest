package com.bonc.location

/**
  * 对位置类型的描述
  * SCECTOR基站小区, GRID栅格/网格, AOI定位到区域多边形
  */
object LocType extends Enumeration  with Serializable {
  type LocType = Value
  val AOI, GRID, SECTOR = Value
}
