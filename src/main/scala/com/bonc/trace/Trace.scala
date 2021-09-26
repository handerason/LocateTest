package com.bonc.trace

import com.bonc.lla.LLA
import com.bonc.location.{LocType, Location}
import com.bonc.location.LocType.LocType
import com.bonc.od.Od
import com.bonc.time.Work
import com.bonc.user.User
import com.bonc.data.{Record, Scene}

import scala.collection.mutable.ListBuffer
import scala.collection.{immutable, mutable}
import scala.util.control.Breaks._

/**
 * 行程路线，即有多个时间上按顺序排列的位置轨迹
 *
 * @param traceType 轨迹类型，可以是CELL/GRID/AOI
 */
class Trace(val traceType: LocType = LocType.GRID) extends Comparable[Trace] with Serializable {
  var imsi: String = _ //用户标识
  var date: String = _ //UTF型日期
  var wheres = new ListBuffer[Where]()
  var pohoneNum = ""

  def this(imsi: String, locType: LocType) {
    this(traceType = locType)
    this.imsi = imsi
  }

  /**
   * 判断trace是否为空
   *
   * @return true:空，false:不空
   */
  def isEmpty(): Boolean = {
    wheres.isEmpty
  }

  override def toString: String = {
    val sb: StringBuilder = new mutable.StringBuilder()
    sb.append(if (imsi != null) imsi else "")
    sb.append(",")
    sb.append(if (traceType != null) traceType else "")
    wheres.foreach(where => {
      sb.append("#")
      sb.append(where)
    })
    sb.toString()
  }


  /**
   * 根据用户的records集合（一天的或者一个小时的或者一次Session的，按照TIME已经排序）
   *
   * @param imsi    用户IMSI
   * @param records records集合，按照时间排序
   */
  def create(imsi: String, records: List[Record]): Unit = {
    //用户标识赋值
    this.imsi = imsi
    //类型赋值
    //对record按照eci是否变化进行分割，如果发生变化则生成新的records
    var preCode: String = null
    var code: String = null
    val recordBuffer = new ListBuffer[Record]()
    for (record <- records) {
      code = record.getCode(traceType)
      if (code != null) { //code为空，则不作处理，对于AOI定位，室外MR会做为空处理
        if (preCode == null || code.equals(preCode)) {
          recordBuffer.append(record)
          preCode = code
        }
        else {
          //生成where
          val where: Where = new Where()
          where.create(recordBuffer.toList, preCode)
          //装载到wheres
          loadAsLast(where)
          //准备继续遍历,先将本record加入缓存
          recordBuffer.clear()
          recordBuffer.append(record)
          preCode = code
        }
      }
    }
    //余下的records生成where
    if (recordBuffer.nonEmpty) {
      //生成where
      val where: Where = new Where()
      where.create(recordBuffer.toList, code)
      //装载到wheres
      append(where)
    }
  }

  def createByWhere(whereList:List[Where]):Unit = {
    for(where <- whereList){
      loadAsLast(where)
    }
  }

  /**
   * 判断Trace是否一直在路上，即所有场景都是ROAD
   *
   * @return TRUE/是， FALSE/否
   */
  def isOnRoad(): Boolean = {
    var isOnRoad: Boolean = true
    breakable {
      wheres.foreach(where => {
        if (where.scene == Scene.INDOOR) {
          isOnRoad = false
          break()
        }
      })
    }
    isOnRoad
  }

  /**
   * 将where装载到wheres的结尾（即要求新装载的where时间上最晚）
   *
   * @param where 新装载的where
   */
  def append(where: Where): Unit = {
    if (wheres.nonEmpty) {
      if (wheres.last.eTime > where.sTime) {
        throw new IllegalStateException("输入的where不是后续where")
      }
      if (wheres.last.code.equals(where.code)) {
        wheres.last.merge(where)
      }
      else { //不需要合并处理
        wheres.append(where)
      }
    }
    else { //如果为空则直接保存
      wheres.append(where)
    }
  }

  /**
   * 根据轨迹类型，判断轨迹中是否包含某个编号的位置点
   *
   * @param code 位置编号
   * @return true/包含，false/不包含
   */
  def contains(code: String): Boolean = {
    var flag = false
    breakable {
      for (where <- wheres) {
        if (where.code.equals(code)) {
          flag = true
          break()
        }
      }
    }
    //    println(flag)
    flag
  }

  /**
   * 根据足迹坐标判断用户是否经过过指定位置，返回经历次数
   * 本函数是根据坐标判断，速度较慢，如果是同一位置层，可以使用
   * contains（code）方法速度会更快
   *
   * @param location 指定位置
   * @return 经历次数
   */
  def passby(location: Location): Int = {
    var passes = 0 //默认经过0次
    wheres.foreach(where => {
      if ((location.lLA.latitude == where.lLA.latitude) && (location.lLA.longitude == where.lLA.longitude)) {
        passes += 1
      }
    })
    passes
  }

  /**
   * 得到用户足迹经历指定位置的总时长
   * 不需要定位计算，用于指定位置与Where同级情况
   *
   * @param location 指定位置
   * @return 经历总时长
   */
  def getTotalTime(location: Location): Long = {
    var totTime: Long = 0 //总时间默认为0
    wheres.foreach(where => {
      if (location.code.equals(where.code)) {
        totTime += where.duration
      }
    })
    totTime
  }

  /**
   * 得到用户足迹经历指定位置的总时长
   * 需要定位计算，用于指定位置与Where非同级情况
   *
   * @param location 指定位置
   * @return 经历总时长
   */
  def getTotalTime1(location: Location): Int = {
    var totTime: Int = 0 //总时间默认为0
    wheres.foreach(where => {
      if ((location.lLA.latitude == where.lLA.latitude) && (location.lLA.longitude == where.lLA.longitude)) {
        totTime += where.duration
      }
    })
    totTime
  }

  /**
   * 根据时间范围返回一个Trace中的子Trace(只要where有一部分在区间内，则提取),注意该时间是绝对时间
   *
   * @param stime 开始时间
   * @param etime 结束时间
   * @return 子Trace
   */
  def getSubTrace(stime: Long, etime: Long): Trace = {
    val subTrace: Trace = new Trace(imsi, traceType) //保存结果
    for (where <- wheres) {
      breakable {
        if (where.sTime >= stime) {
          if (where.eTime <= etime) {
            subTrace.wheres.append(where)
          } else {
            break()
          }
        }
      }
    }
    subTrace
  }

  /**
   * 得到列表中的OD对间的子Trace，提取出所有的OD对子Trace(可能是多个Trace,用户多次途径)
   *
   * @param sCode O位置编码
   * @param eCode D位置编码
   * @return OD对子Trace列表
   */
  // 第一个where先进来，第一个where不是scode，直接swhere为空跳过，第二个where过来，是scode，直接进入第一个if，swhere有值了，从而后边不一样的scode就进入第三个if的else，添加进去，之后再去遍历，遇到
  // ecode之后再将swhere设置为null
  // 因为用户可能出现多次从A到B的情况，所以可能出现多个Trace
  def getSubTraces(sCode: String, eCode: String): ListBuffer[Trace] = {
    val subTraces = new ListBuffer[Trace] //保存结果
    var subTrace: Trace = null //初始化为null
    var sWhere: Where = null //标记sCode对应的where
    for (where <- this.wheres) {
      //如果where的编码==sCode则认为找到swhere
      if (where.code.equals(sCode)) {
        //生成子trace,并装载where
        subTrace = new Trace(imsi, traceType)
        subTrace.date = this.date
        subTrace.wheres.append(where)
        sWhere = where //标记
      }
      else { //首先找到swhere,然后再找ewhere
        if (sWhere != null) {
          //如果找到，则生成元祖，放到结果列表中
          if (where.code.equals(eCode)) {
            subTrace.wheres.append(where)
            subTraces.append(subTrace) //装载
            sWhere = null //准备下一次搜索
          }
          else {
            subTrace.wheres.append(where)
          }
        }
      }
    }
    subTraces
  }

  /**
   * 计算得到本TRace的轨迹曼哈顿距离
   *
   * @return 曼哈顿轨迹距离
   */
  def mhDistance(): Double = {
    var distance: Double = 0.0 //距离初始化为0
    var sWhere: Where = null
    for (where <- wheres) {
      if (sWhere == null) {
        sWhere = where //初始化为当前位置
      }
      else {
        distance += sWhere.getMhDistance(where)
        sWhere = where //初始化为当前位置
      }
    }
    distance
  }

  /**
   * 计算得到本TRace的轨迹欧几里得距离
   *
   * @return 欧几里得轨迹距离
   */
  def distance(): Double = {
    var distance: Double = 0.0 //距离初始化为0
    var sWhere: Where = null
    for (where <- wheres) {
      if (sWhere == null) {
        sWhere = where //初始化为当前位置
      }
      else {
        distance += sWhere.getDistance(where)
        sWhere = where //初始化为当前位置
      }
    }
    distance
  }

  /**
   * 时间段内的曼哈顿距离
   *
   * @param sTime 起始时间
   * @param eTime 结束时间
   * @return 曼哈顿距离
   */
  def getMhDistance(sTime: Long, eTime: Long): Double = {
    getSubTrace(sTime, eTime).mhDistance()
  }

  /**
   * 时间段内的欧几里得距离
   *
   * @param sTime 起始时间
   * @param eTime 结束时间
   * @return 欧几里得距离
   */
  def getDistance(sTime: Long, eTime: Long): Double = {
    getSubTrace(sTime, eTime).distance()
  }

  /**
   * trace耗时
   *
   * @return 总时间
   */
  def costTime(): Int = {
    (wheres.last.sTime - wheres.head.eTime).toInt
  }

  /**
   * 指定位置间的trace耗时，如果OD位置其一为空，则返回最大值
   *
   * @param sCode 起始位置编码
   * @param eCode 结束位置编码
   * @return 耗时
   */
  def costTime(sCode: String, eCode: String): Int = {
    val subTraces = getSubTraces(sCode, eCode)
    var time: Int = 0 //初始化为0
    if (subTraces.nonEmpty) {
      for (subTrace <- subTraces) {
        time += subTrace.costTime()
      }
    }
    if (time > 0) time / subTraces.length
    else Int.MaxValue //否则返回最大值
  }

  /**
   * 合并两个trace对象，两个trace只能首尾有相同，不重叠（用于两天的trace合并）(改)
   *
   * @param trace 待合并轨迹
   */
  def merge(trace: Trace): Unit = {
    //如果两个位置处于同一个位置，则合并
    if (!imsi.equals(trace.imsi)) {
      throw new IllegalArgumentException("输入的trace与当前trace不属于同一用户！")
    }
    //    println(wheres.head.sTime)
    //    println(com.bonc.trace.wheres.last.eTime)
    //合并操作
    if (wheres.last.eTime < trace.wheres.head.sTime) {
      //      println(wheres.last.code)
      //      println(com.bonc.trace.wheres.head.code)
      if (wheres.last.code.equals(trace.wheres.head.code)) {
        // 合并同一位置
        // 这里的trace是一个个OD集合，这里trace中的wheres中相邻的where肯定是不同的code，若是前后两个trace的头尾where一致，就先合并，之后将除了头之外的所有元素添加
        wheres.last.merge(trace.wheres.head)
        for (i <- trace.wheres.tail) {
          wheres.append(i)
        }
      }
      else {
        for (i <- trace.wheres) {
          wheres.append(i)
        }
      }
    }
    else if (wheres.head.sTime > trace.wheres.last.eTime) {
      //      println("in")
      if (wheres.head.code.equals(trace.wheres.last.code)) {
        trace.wheres.last.merge(wheres.head)
        for (i <- wheres.tail) {
          trace.wheres.append(i)
        }
        wheres = trace.wheres
      }
      else {
        for (i <- wheres) {
          trace.wheres.append(i)
        }
        wheres = trace.wheres
      }
    }
  }

  /**
   * 根据输入的编号，查询trace中具有相同编号的where
   *
   * @param code 编号
   * @return 编号相同的wheres
   */
  def getWheres(code: String): List[Where] = {
    val resWheres = new ListBuffer[Where]()
    wheres.foreach(where => {
      if (where.code.equals(code)) {
        resWheres.append(where)
      }
    })
    resWheres.toList
  }

  /**
   * 提取居住地到工作地的轨迹
   *
   * @param user 用户信息
   * @return 居住地到工作地的轨迹
   */
  def getHome2WorkTraces(user: User, locType: LocType): List[Trace] = {
    var traces: ListBuffer[Trace] = new ListBuffer[Trace]
    if (locType.equals(LocType.AOI)) {
      traces = getSubTraces(user.liviPlace._1, user.workPlace._1)
    } else if (locType.equals(LocType.GRID)) {
      traces = getSubTraces(user.liviPlace._2, user.workPlace._2)
    } else if (locType.equals(LocType.SECTOR)) {
      traces = getSubTraces(user.liviPlace._3, user.workPlace._3)
    }
    traces.toList
  }

  /**
   * 提取工作地到居住地的轨迹
   *
   * @param user 用户信息
   * @return 工作地到居住地的轨迹
   */
  def getWork2HomeTraces(user: User, locType: LocType): List[Trace] = {
    var traces: ListBuffer[Trace] = new ListBuffer[Trace]
    if (locType.equals(LocType.AOI)) {
      traces = getSubTraces(user.workPlace._1, user.liviPlace._1)
    } else if (locType.equals(LocType.GRID)) {
      traces = getSubTraces(user.workPlace._2, user.liviPlace._2)
    } else if (locType.equals(LocType.SECTOR)) {
      traces = getSubTraces(user.workPlace._3, user.liviPlace._3)
    }
    traces.toList
  }

  /**
   * 计算家到工作地的通勤时间
   *
   * @param user 用户信息
   * @return 家到工作地的通勤时间
   */
  def getHome2WorkTime(user: User, locType: LocType): List[Int] = {
    //保存通勤时长结果
    val durations = new ListBuffer[Int]()
    //从trace中提取居住地到工作地的trace
    val traces = getHome2WorkTraces(user, locType)
    for (trace <- traces) {
      if (!trace.isEmpty()) {
        //第一个位置的出发时间到最后一个位置的到达时间
        durations.append((trace.wheres.last.sTime - trace.wheres.head.eTime).toInt)
      }
    }
    durations.toList
  }

  /**
   * 计算工作地到居住地的通勤时间
   *
   * @param user 用户信息
   * @return 工作地到居住地的通勤时间
   */
  def getWork2HomeTime(user: User, locType: LocType): List[Int] = {
    //保存通勤时长结果
    val durations = new ListBuffer[Int]()
    //从trace中提取居住地到工作地的trace
    val traces = getWork2HomeTraces(user, locType)
    for (trace <- traces) {
      if (!trace.isEmpty()) {
        //第一个位置的出发时间到最后一个位置的到达时间
        durations.append((trace.wheres.last.sTime - trace.wheres.head.eTime).toInt)
      }
    }
    durations.toList
  }

  /**
   * 计算1天的OD,从家里出发到经留时间最长的工作时间内目的地（改）
   *
   * @param user 用户,主要用其居住地标识
   * @return od 对象
   */
  def get1DayOd(user: User, minStayTime: Int = 60, locType: LocType, work: Work): Option[Od] = {
    //截取trace的开始时间确定
    var sTime: Long = 0
    //得到居住地出现的Where及第一次结束时间
    var homeCode = ""
    if (locType.equals(LocType.AOI)) {
      homeCode = user.liviPlace._1
    } else if (locType.equals(LocType.GRID)) {
      homeCode = user.liviPlace._2
    } else if (locType.equals(LocType.SECTOR)) {
      homeCode = user.liviPlace._3
    }
    val wheres: immutable.Seq[Where] = getWheres(homeCode)
    if (wheres.nonEmpty) {
      sTime = wheres.head.eTime
    }
    //    println(sTime)
    //    println(work.sTime(date))
    //限定sTime在工作时间之内
    sTime = Math.min(sTime, work.sTime(date)) //需要转换成Long型
    val subTrace: Trace = getSubTrace(sTime, work.eTime(date))
    //    println(subTrace.toString)
    val dWhere = subTrace.seekMaxDWhere(user, minStayTime, locType)
    //    println(dWhere)
    dWhere match {
      //      case Some(where) => new Od(wheres.head, where.get)
      case Some(where) =>
        val od = new Od()
        od.updateOd(user.imsi, wheres.head, where)
        Some(od)
      case None => None
    }
  }

  /**
   * 剔除居住地之外的时间最长且超过1小时的为出行目的地（改）
   *
   * @param user 用户信息
   * @return
   */
  def seekMaxDWhere(user: User, minTimeThr: Int = 60, locType: LocType): Option[Where] = {
    //保存每个位置的总经留时间
    val totTimes = new mutable.HashMap[String, Int]
    //搜查非居住地时间最长的外出地点，且时间大于指定门限
    var homeCode = ""
    if (locType.equals(LocType.AOI)) {
      homeCode = user.liviPlace._1
    } else if (locType.equals(LocType.GRID)) {
      homeCode = user.liviPlace._2
    } else if (locType.equals(LocType.SECTOR)) {
      homeCode = user.liviPlace._3
    }
    wheres.foreach(where => {
      //      println(where.duration)
      if (!where.code.equals(homeCode)) {
        val time = totTimes.get(where.code)
        time match {
          case Some(x) => totTimes.put(where.code, x + where.duration)
          case None => totTimes.put(where.code, where.duration)
        }
      }
    })
    //    println(totTimes)
    //时间最长的外出地点，且时间大于指定门限
    val time = totTimes.maxBy((time: (String, Int)) => time._2)
    //返回最长时间对应的where
    if (time._2 > minTimeThr * 60 * 1000) { //time的单位是毫秒
      Some(wheres.filter((_: Where).code.equals(time._1)).head)
    } else {
      None
    }
  }


  /**
   * 统计一个用户的从居住地到工作地的通勤时长
   *
   * @param traces 统计期间内的traces，一般是一天一个trace
   * @param user   用户信息
   * @return 平均通勤时长
   */
  def avgHome2WorkTime(traces: List[Trace], user: User, locType: LocType): Int = {
    val times = new ListBuffer[Int]()
    for (trace <- traces) {
      val costTimes: List[Int] = trace.getHome2WorkTime(user, locType)
      for (i <- costTimes) {
        times.append(i)
      }
    }
    //对通勤时间进行处理，取中位数
    if (times.size % 2 == 0) (times(times.size / 2 - 1) + times(times.size / 2)) / 2
    else times(times.size % 2)
  }

  /**
   * 统计一个用户的从工作地到居住地的通勤时长
   *
   * @param traces 统计期间内的traces，一般是一天一个trace
   * @param user   用户信息
   * @return 平均通勤时长
   */
  def avgWork2HomeTime(traces: List[Trace], user: User, locType: LocType): Int = {
    val times = new ListBuffer[Int]()
    for (trace <- traces) {
      val costTimes: List[Int] = trace.getWork2HomeTime(user, locType)
      for (i <- costTimes) {
        times.append(i)
      }
    }
    //对通勤时间进行处理，取中位数
    if (times.size % 2 == 0) (times(times.size / 2 - 1) + times(times.size / 2)) / 2
    else times(times.size % 2)
  }

  /**
   * 新添加的方法,这个方法用来过滤出在Trace的OD中属于某个地点的OD
   *
   * @param code 限定的地点
   * @return List[Where] 过滤完成的where集合
   */
  def getLocations(code: String): List[Where] = {
    val wheres_filter = new ListBuffer[Where]
    for (i <- wheres) {
      if (i.code.equals(code)) {
        wheres_filter.append(i)
      }
    }
    wheres_filter.toList
  }

  /**
   * 向wheres的尾部添加元素
   *
   * @param where 需要尾部添加的元素
   */
  def loadAsLast(where: Where): Unit = {
    if (wheres.nonEmpty) {
      if (wheres.last.eTime > where.sTime) {
        throw new IllegalStateException("输入的where在时间上不是连续的。wheres.last.sTime="+wheres.last.sTime+";where.last.eTime="+wheres.last.eTime+";where.sTime:"+where.sTime+";where.eTime:"+where.eTime+";imsi="+imsi)
      }
      if (wheres.last.code.equals(where.code)) {
        wheres.last.merge(where)
      } else {
        wheres.append(where)
      }
    } else {
      wheres.append(where)
    }

  }

  override def compareTo(o: Trace): Int = {
    var i: Int = 0
    if (o.imsi.equals(this.imsi)) {
      i = o.date.compareTo(this.date)
    }
    i
  }
}


object Trace {
  def main(args: Array[String]): Unit = {
    val record1 = new Record
    record1.lLA = new LLA(11, 21)
    record1.imsi = "imsi1"
    record1.sTime = 1599617105000L
    record1.eTime = 1599624305000L
    record1.scene = Scene.INDOOR
    record1.eci = 20000l
    record1.gridCode = "gridcode1"
    record1.aoiCode = "aoaCode3"
    record1.session = (Math.random() * 10000).toInt

    val record2 = new Record
    record2.lLA = new LLA(12, 22)
    record2.imsi = "imsi1"
    record2.sTime = 1599624306000L
    record2.eTime = 1599627905000L
    record2.scene = Scene.INDOOR
    record2.eci = 20000l
    record2.gridCode = "gridcode2"
    record2.aoiCode = "aoaCode3"
    record2.session = (Math.random() * 10000).toInt

    val record3 = new Record
    record3.lLA = new LLA(13, 23)
    record3.sTime = 1599627906000L
    record3.eTime = 1599635105000L
    record3.scene = Scene.INDOOR
    record3.eci = 20000l
    record3.gridCode = "gridcode3"
    record3.aoiCode = "aoaCode3"
    record3.imsi = "imsi1"
    record3.session = (Math.random() * 10000).toInt

    val record4 = new Record
    record4.lLA = new LLA(13, 23)
    record4.sTime = 1599635106000L
    record4.eTime = 1599642305000L
    record4.scene = Scene.INDOOR
    record4.eci = 20000l
    record4.gridCode = "gridcode3"
    record4.aoiCode = "aoaCode4"
    record4.imsi = "imsi1"
    record4.session = (Math.random() * 10000).toInt

    val record5 = new Record
    record5.lLA = new LLA(13, 23)
    record5.sTime = 1599649505000L
    record5.eTime = 1599660305000L
    record5.scene = Scene.INDOOR
    record5.eci = 20000l
    record5.gridCode = "gridcode3"
    record5.aoiCode = "aoaCode1"
    record5.imsi = "imsi1"
    record5.session = (Math.random() * 10000).toInt

    val records = new ListBuffer[Record]
    records.append(record1)
    records.append(record2)
    records.append(record3)
    records.append(record4)
    records.append(record5)

    val trace = new Trace(LocType.AOI)
    trace.create("imsi1", records.toList)
    println(trace)

    println(trace.isOnRoad())

    println(trace.contains("aoaCode1"))

    val location = new Location
    location.lLA = new LLA(8.000003202161347, 22.99999784146678)
    println(trace.passby(location))

    location.code = "aoaCode3"
    println(trace.getTotalTime(location))

    println(trace.getTotalTime1(location))

    val trace1: Trace = trace.getSubTrace(1599624690000L, 1599628891000L)
    println(trace1.toString)

    val traces: ListBuffer[Trace] = trace.getSubTraces("aoaCode1", "aoaCode2")
    for (i <- traces) {
      println(i.toString)
    }

    println(trace.costTime())

    val where = new Where()
    where.sTime = 1599595505000L
    where.eTime = 1599599105000L
    where.code = "aoaCode1"
    where.scene = Scene.INDOOR
    where.lLA = new LLA(11, 21)
    val where1 = new Where()
    where1.sTime = 1599606305000L
    where1.eTime = 1599613505000L
    where1.code = "aoaCode5"
    where1.lLA = new LLA(10, 20)
    val wheres = new ListBuffer[Where]()
    wheres.append(where)
    wheres.append(where1)
    val trace2 = new Trace(LocType.AOI)
    trace2.wheres = wheres
    trace2.imsi = "imsi1"
    trace2.date = "20200909"
    println(trace2)
    trace.merge(trace2)
    println(trace)

    val wheres1: List[Where] = trace.getWheres("aoaCode1")
    for (i <- wheres1) {
      println(i)
    }

    val user = new User
    user.imsi = "imsi1"
    user.liviPlace = ("aoaCode1", "", "")
    user.workPlace = ("aoaCode3", "", "")
    val traces1: List[Trace] = trace.getWork2HomeTraces(user, LocType.AOI)
    for (i <- traces1) {
      println(i)
    }

    val ints: List[Int] = trace.getHome2WorkTime(user, LocType.AOI)
    for (i <- ints) {
      println(i)
    }

    val work = new Work
    work.sHour = 8D
    work.eHour = 18D
    trace.date = "20200909"
    val maybeOd: Option[Od] = trace.get1DayOd(user, 60, LocType.AOI, work)
    maybeOd match {
      case Some(od) => println(od.toString)
      case None => println("none")
    }

    val where3 = new Where()
    where3.sTime = 1599595505000L
    where3.eTime = 1599599105000L
    where3.code = "aoaCode1"
    where3.scene = Scene.INDOOR
    where3.lLA = new LLA(11, 21)
    val where4 = new Where()
    where4.sTime = 1599606305000L
    where4.eTime = 1599613505000L
    where4.code = "aoaCode3"
    where4.lLA = new LLA(10, 20)
    val where5 = new Where()
    where5.sTime = 1599606305000L
    where5.eTime = 1599613505000L
    where5.code = "aoaCode3"
    where5.lLA = new LLA(10, 20)
    val wheresx = new mutable.TreeSet[Where]()
    wheresx.add(where3)
    wheresx.add(where4)
    wheresx.add(where5)
    val trace3 = new Trace(LocType.AOI)
    trace3.wheres = wheres
    trace3.imsi = "imsi1"
    trace3.date = "20200909"

    val traces2 = new ListBuffer[Trace]
    traces2.append(trace)
    traces2.append(trace3)
    val time: Int = trace2.avgWork2HomeTime(traces2.toList, user, LocType.AOI)
    println(time)

    val wheres2: List[Where] = trace.getLocations("aoaCode1")
    for (i <- wheres2) {
      println(i.toString)
    }

  }

}























