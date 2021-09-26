package com.bonc.data

/**
 * 枚举类表示定位类型，主要定位类型包括MR定位、XDR定位和MDT定位（其中OTT定位归属到MDT定位）
 */

object LocateType extends Enumeration with Serializable {
  //声明枚举对外暴露的变量类型
  type LocateType = Value
  //MR定位, XDR定位, 经纬度定位
  val MR, XDR, MDT = Value
}
