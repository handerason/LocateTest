package com.bonc.network

/**
  * 网络类型
  */
object NetworkType extends Enumeration with Serializable {
  type NetworkType = Value //声明枚举对外暴露的变量类型
  val G3, G4, G5 = Value //分别是23G/4G/5G网络
}
