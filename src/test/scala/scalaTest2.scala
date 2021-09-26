import com.bonc.data.Record
import com.bonc.lla.LLA
import org.apache.commons.lang.math.NumberUtils
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * @Author: whzHander 
 * @Date: 2021/6/9 13:58 
 * @Description:
 */
object scalaTest2 {
  def main(args: Array[String]): Unit = {
    val hour = "2021080801"
    val date = hour.substring(0,2)
    println(date)
//    val LLAs = new ListBuffer[String]
//    LLAs.append("1")
//    LLAs.append("2")
//    LLAs.append("3")
//    LLAs.append("4")
//    LLAs.append("5")
//    println(LLAs.toString())
  }
}
