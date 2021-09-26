package com.bonc.trace

/**
  * WHERE的属性，分别是居住地HOME，工作地WORK，居住地兼工作地H&W，
  * 驻留点LINGER和途经点PASSBY
  */
object WhereType extends Enumeration with Serializable {
  type WhereType = Value
  val LIVE, WORK, WORKLIVE, LINGER, PASSBY = Value
  var sTime: Double = 9.0 //一天中区间的的开始时间
  var eTime: Double = 17.0 //一天中区间的结束时间
  var duration: Double = 0.5 //默认30分钟

  /**
    * 在用户一天的trace中寻找1天居住地
    * @param trace 用户一天的trace
    * @return 1天居住地编码
    */
  def seek1DayLIVE(trace: Trace): String ={
    //用户在每天21:00至次日5:00累计驻留时间最长的位置标记为当日居住地
    /**************此处写代码*************************/
    ""
  }
  /**
    * 在用户一天的trace中寻找1天工作地
    * @param trace 用户一天的trace
    * @return 1天工作地编码
    */
  def seek1DayWORK(trace: Trace): String ={
    //用户在每天 9:00 至 17:00 累计驻留时间最长的位置标记为当日工作地，
    /**************此处写代码*************************/
    ""
  }


  /**
    * 识别trace中每个Where的类型，包括当日居住地/工作地/驻留点和途经点
    * @param trace 用户轨迹
    */
  def recognize(trace: Trace): Unit ={
    //每日工作地和居住地直接调用或通过之前定义识别
    //在where中的wType进行赋值LIVE/WORK
    //用户在某地驻留时长超过 30min
    //在where中的wType进行赋值LINGER
    //其他的是途经点
    //赋值 PASSBY
  }
}
