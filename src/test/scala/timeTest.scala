import java.text.SimpleDateFormat

import org.apache.calcite.util.NumberUtil
import org.apache.commons.lang.math.NumberUtils

/**
 * @Author: whzHander 
 * @Date: 2021/7/22 14:50 
 * @Description:
 */
object timeTest {
  def main(args: Array[String]): Unit = {
    val tm = "20210827"
    val tm2 = "2021-07-22 00:01:18.243"
    val eTime = tranTimeToLong(tm)
    val a = tranTimeToLong(tm)
//1626883260000
    println(eTime)
//    println(longToStringMinute(1628531417000L))
//    println(longToTranTime(1627714600960L))
//    println(longToTranTime(1627714703000L))
//    println(longToTranTime(1627714703360L))
//    println(longToTranTime(1627714708480L))
//    println(longToTranTime(1627715153920L))
//    println(longToTranTime(1627715665920L))


  }
  def tranTimeToLong(tm:String) :Long={
    val fm = new SimpleDateFormat("yyyyMMdd")
    val dt = fm.parse(tm)
    val tim: Long = dt.getTime()
    tim
  }
  def longToTranTime(tm:Long) :String={
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val lm = fm.format(tm)
    lm
  }
  def longToStringMinute(tm: Long): String = {
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm")
    val lm = fm.format(tm)
    lm
  }
}
