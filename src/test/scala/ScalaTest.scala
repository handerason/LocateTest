import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter

/**
 * @Author: whzHander 
 * @Date: 2021/5/26 19:31 
 * @Description:
 */
object ScalaTest {
  def main(args: Array[String]): Unit = {


    val time = "2021-05-24"
    val newtime = LocalDate.parse(time)

    val nowDate = LocalDate.now()

    val tm = "1502036122000"
    val a = tranTimeToString(tm)
    println(a)

    val dateFormat: Long = new SimpleDateFormat("yyyyMMdd").parse("20210524").getTime
    println("dateFormat:"+dateFormat)
    val secondTime = tranTimeToString(dateFormat.toString)
    val week = LocalDate.parse(secondTime).getDayOfWeek

    println("***************************")
    println("secondTime:"+secondTime)
    println("week:"+week)
    println("***************************")

//    val dateFormat: String = new SimpleDateFormat("HH").parse(time).toString
//    println(dateFormat)
//    val dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
//    val result = LocalDateTime.parse(time, dtf)
//
//    println(result)
    println("20210524星期几："+newtime.getDayOfWeek)

//    println("当前日期是：" + nowDate) //2020-08-14
//
//    println("明天日期是：" + nowDate.plusDays(1))
    println("昨天日期是：" + newtime.plusDays(-1))
//
//    println("当前日期加一个月是：" + nowDate.plusMonths(1))
//    println("当前日期减一个月是：" + nowDate.plusMonths(-1))
//
//    println("今天是今年的第几天：" + nowDate.getDayOfYear)
//    println("今天是当月的第几天：" + nowDate.getDayOfMonth)
//    println("今天星期几：" + nowDate.getDayOfWeek)
//    println("这个月是：" + nowDate.getMonth)

    /*
当前日期是：2020-08-14
明天日期是：2020-08-15
昨天日期是：2020-08-13
当前日期加一个月是：2020-09-14
当前日期减一个月是：2020-07-14
今天是今年的第几天：227
这个月有多少天：14
今天星期几：FRIDAY
这个月是：AUGUST
    **/


    val nowDT = LocalDateTime.now()
    println(nowDT)

    val dt = nowDT.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    println(dt)


//    val dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
//    val result = LocalDateTime.parse("2020-08-08 18:28:38", dtf)
//    println(result)

    /*
2020-08-14T16:18:50.472
2020-08-14 16:18:50
2020-08-08T18:28:38
    **/
    val datees = "2017-04-14 07:29:03"
    val hour = tranfTime(datees)
    println(hour)
  }
  def tranfTime(timestring: String): Int = {
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //将string的时间转换为date
    val time: Date = fm.parse(timestring)

    val cal = Calendar.getInstance()
    cal.setTime(time)
    //提取时间里面的小时时段
    val hour: Int = cal.get(Calendar.HOUR_OF_DAY)
    hour
  }
  def tranTimeToString(tm:String) :String= {
    val fm = new SimpleDateFormat("yyyy-MM-dd")
    val tim = fm.format(new Date(tm.toLong))
    tim
  }
}
