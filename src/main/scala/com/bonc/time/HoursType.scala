package com.bonc.time

/**
  * 一天内的时间类型，分为工作时间和居家时间
  */
object HoursType extends Enumeration with Serializable {
  type HoursType = Value
  val HOME, WORK = Value//居家时间和工作时间
}
