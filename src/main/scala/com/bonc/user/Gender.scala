package com.bonc.user

/**
  * 用户性别标签
  * Male:男人， Female：女人
  */
object Gender extends Enumeration with Serializable {
  type Gender = Value
  val Male, Female = Value
}
