package com.bonc.ServerTest

import com.bonc.lla.LLA
import com.bonc.location.LocType
import com.bonc.trace.{Trace, Where}
import com.bonc.user_locate.ReadParam
import com.bonc.user_locate.utils.{FileUtil, LocateUtils}
import mrLocateV2.bsparam.{Cell, EarfcnPciUnitedKey}
import org.apache.commons.lang.math.NumberUtils
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import java.io.BufferedReader
import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}
import java.{lang, util}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * @deprecated 出行客户群体识别,教务工作者、学生、医务人员、商务人员
 * @author opslos
 * @date 2021/1/4 20:53:39
 */
object customerGroupIdentifyModel2 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("com").setLevel(Level.WARN)

    if (args.length < 2) {
      println("args must be at least 2")
      System.exit(1)
    }
    val configPath = args(0) // 配置文件路径，与jar包相同目录
    val date = args(1) // 日期

    val conf = new SparkConf()
    ReadParam.readXML(conf, configPath)
    conf.set("spark.sql.shuffle.partitions", "1000")
    conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    conf.set("date", date)
    conf.set("spark.yarn.queue", conf.get("queuename"))
    conf.setAppName("user_trace" + date)
    conf.set("spark.driver.maxResultSize", "20g")
    // 使用kryo序列化
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array[Class[_]](classOf[Nothing]))

    //判断是否启用本地模式
    val isLocal: String = conf.get("local")
    if ("1".equals(isLocal)) {
      conf.setMaster("local[*]")
    }

    val sc = new SparkContext(conf) // 创建SparkContext

    var outputPath: String = conf.get("outputPath")
    var userLiveWorkInput: String = conf.get("userLiveWorkInput") //用户职驻地数据
    var userAgeIncomeInput: String = conf.get("userAgeIncomeInput") // 用户年龄收入
    var userIncomeEducation: Int = NumberUtils.toInt(conf.get("userIncomeEducation"))  // 教育工作者资费
    var userBusinessIncome: Int = NumberUtils.toInt(conf.get("userBusinessIncome"))  // 教育工作者资费


    val cellMap = new util.HashMap[java.lang.Long, Cell]()
    val eciMap = new util.HashMap[Integer, util.HashMap[EarfcnPciUnitedKey, java.lang.Long]]
    LocateUtils.readBs2Cache(sc, isLocal, conf.get("site", ""), cellMap, eciMap)
    System.out.println("cellMap---" + cellMap.size())
    System.out.println("eciMap---" + eciMap.size())

    // 这里将所有指定的学校区域
    val schoolReader: BufferedReader = FileUtil.getBufferedReader(new Path(conf.get("APP_DIM_ECI_ALL_CITY_COUNTY_CODE")))
    val schoolMap = new mutable.HashMap[String, ListBuffer[String]]()
    var school: String = ""
    school = schoolReader.readLine()
    while (school != null) {
      if (school.contains("贵州大学")){
        if (!schoolMap.keys.toList.contains("贵州大学")){
          val strings = new ListBuffer[String]
          strings.append(school.split("\t")(0))
          schoolMap.put("贵州大学",strings)
        }else{
          schoolMap("贵州大学").append(school.split("\t")(0))
        }
      }
      school = schoolReader.readLine()
    }
    schoolReader.close()
    println("schoolList:" + schoolMap.size)
    val schoolBroad: Broadcast[mutable.HashMap[String, ListBuffer[String]]] = sc.broadcast(schoolMap)

    // 这里将所有指定的医院区域
    val hospitalReader: BufferedReader = FileUtil.getBufferedReader(new Path(conf.get("APP_DIM_ECI_ALL_CITY_COUNTY_CODE")))
    val hospitalMap = new mutable.HashMap[String, ListBuffer[String]]()
    var hospital: String = ""
    hospital = hospitalReader.readLine()
    while (hospital != null) {
      if (hospital.contains("荣军康复医院")){
        if (!hospitalMap.keys.toList.contains("荣军康复医院")){
          val strings = new ListBuffer[String]
          strings.append(hospital.split("\t")(0))
          hospitalMap.put("荣军康复医院",strings)
        }else{
          hospitalMap("荣军康复医院").append(hospital.split("\t")(0))
        }
      }
      hospital = hospitalReader.readLine()
    }
    hospitalReader.close()
    println("hospitalList" + hospitalMap.size)
    val hospitalBroad: Broadcast[mutable.HashMap[String, ListBuffer[String]]] = sc.broadcast(hospitalMap)

    // 这里将所有指定的商务区域
    val businessReader: BufferedReader = FileUtil.getBufferedReader(new Path(conf.get("APP_DIM_ECI_ALL_CITY_COUNTY_CODE")))
    val businessMap = new mutable.HashMap[String, ListBuffer[String]]()
    var business: String = ""
    business = businessReader.readLine()
    while (business != null) {
      if (business.contains("富力")){
        if (!businessMap.keys.toList.contains("富力")){
          val strings = new ListBuffer[String]
          strings.append(business.split("\t")(0))
          businessMap.put("富力",strings)
        }else{
          businessMap("富力").append(business.split("\t")(0))
        }
      }
      business = businessReader.readLine()
    }
    businessReader.close()
    println("businessList" + businessMap.size)
    val businessBroad: Broadcast[mutable.HashMap[String, ListBuffer[String]]] = sc.broadcast(businessMap)

    //    手机号码 imsi imei 统计日期 常驻地城市
    //    工作地所在城市 工作地所在区县 工作地基站纬度 工作地基站经度 工作地基站小区 居住地所在城市 居住地所在区县 居住地基站纬度 居住地基站经度 居住地基站小区
    //    周末驻留地所在城市 周末驻留地所在区县 周末驻留地基站纬度 周末驻留地基站经度 周末驻留地基站小区
    val userLiveWorkPlace: RDD[(String, String, String, String, String, String, String)] = sc.textFile(userLiveWorkInput).map(x => {
      val strings: Array[String] = x.split("\t")
      (strings(0), strings(1), strings(2), strings(3), strings(9), strings(14), strings(19))
    })

    // 获取所有人的资费和年龄
    val userAgeIncomeList: List[(String, String, String, String, String)] = sc.textFile(userAgeIncomeInput).map(x => {
      val strings: Array[String] = x.split("\t")
      // 手机号，imsi，imei，age，收入
      (strings(0), strings(1), strings(2), strings(3), strings(4))
    }).collect().toList


    //    （四）教务工作者
    //    工作地网络小区为学校覆盖网络小区（根据小区的名称判断，附近小区根据距离判断，或者通过人工定义小区集合），年龄大于23岁
    //    工作地与居住地不在一起时，周六周日在工作地的概率小于50%（排除学校附近的工作人员）。当工作地和居住地都是学校，则要求资费大于某值（区别与学校附近的家庭主妇或老人）。
    val educationUser: RDD[(String, String, String, String, String, String, String)] = userLiveWorkPlace.filter(x => {
      val schoolMap: mutable.HashMap[String, ListBuffer[String]] = schoolBroad.value
      // 根据小区集合来判断工作地小区是否为学校覆盖的小区
      schoolMap.exists(y => y._2.contains(x._5))
    }).filter(x => {
      var bool = false
      val schoolMap: mutable.HashMap[String, ListBuffer[String]] = schoolBroad.value
      var workPlace = ""
      var LivePlace = ""
      var holidayPlace = ""
      val work: String = x._5
      val live: String = x._6
      val holidayStay: String = x._7
      if (schoolMap.exists(y => y._2.contains(work))) {
        workPlace = schoolMap.filter(z => z._2.contains(work)).head._1
      }
      if (schoolMap.exists(y => y._2.contains(live))) {
        LivePlace = schoolMap.filter(z => z._2.contains(live)).head._1
      }
      if (schoolMap.exists(y => y._2.contains(holidayStay))) {
        holidayPlace = schoolMap.filter(z => z._2.contains(holidayStay)).head._1
      }
      // 工作地与居住地不在一起时，周六周日在工作地的概率小于50%（排除学校附近的工作人员）。当工作地和居住地都是学校，则要求资费大于某值（区别与学校附近的家庭主妇或老人）。
      if (live != "" && work != "" && !live.equals(work)) {
        if (holidayPlace != "" && !workPlace.equals(holidayPlace)) {
          bool = true
        }
      } else if (live != "" && work != "" && live.equals(work)) {
        val filter: List[(String, String, String, String, String)] = userAgeIncomeList.filter(_._1.equals(x._1))
        if (filter.nonEmpty) {
          val income: Int = NumberUtils.toInt(filter.head._5)
          bool = income > userIncomeEducation
        }
      }
      bool
    })

    //    （五）学生
    //    工作地小区为学校附近小区（根据小区的名称判断，附近小区根据距离判断），且周六周日年龄小于23岁。
    val studentUser: RDD[(String, String, String, String, String, String, String)] = userLiveWorkPlace.filter(x => {
      var bool = false
      val schoolMap: mutable.HashMap[String, ListBuffer[String]] = schoolBroad.value
      var workPlace = ""
      var LivePlace = ""
      val work: String = x._5
      val live: String = x._6
      if (schoolMap.exists(y => y._2.contains(work))) {
        workPlace = schoolMap.filter(z => z._2.contains(work)).head._1
      }
      if (schoolMap.exists(y => y._2.contains(live))) {
        LivePlace = schoolMap.filter(z => z._2.contains(live)).head._1
      }
      val filter: List[(String, String, String, String, String)] = userAgeIncomeList.filter(_._1.equals(x._1))
      if (filter.nonEmpty) {
        val age: Int = NumberUtils.toInt(filter.head._4)
        bool = age < 23 && LivePlace != "" && workPlace != ""
      }
      bool
    })

    //    （六）医务工作者
    //    工作地为医院附近网络小区，年龄大于18岁。居住地与工作地为不同网络小区。周六周日常驻小区不在工作地小区的概率大于50%（区别于附近的家庭主妇或老人，或者长期住院病人）。
    // 工作居住的基站不同
    val doctorUser: RDD[(String, String, String, String, String, String, String)] = userLiveWorkPlace.filter(x => {
      var bool = false
      val hospitalMap: mutable.HashMap[String, ListBuffer[String]] = hospitalBroad.value
      var workPlace = ""
      var LivePlace = ""
      var holidayPlace = ""
      val work: String = x._5
      val live: String = x._6
      val holidayStay: String = x._7
      if (hospitalMap.exists(y => y._2.contains(work))) {
        workPlace = hospitalMap.filter(z => z._2.contains(work)).head._1
      }
      if (hospitalMap.exists(y => y._2.contains(live))) {
        LivePlace = hospitalMap.filter(z => z._2.contains(live)).head._1
      }
      if (hospitalMap.exists(y => y._2.contains(holidayStay))) {
        holidayPlace = hospitalMap.filter(z => z._2.contains(holidayStay)).head._1
      }
      if (workPlace != "" && workPlace != LivePlace) {
        bool = (NumberUtils.toInt(work) / 256 != NumberUtils.toInt(live) / 256) && (NumberUtils.toInt(work) / 256 != NumberUtils.toInt(holidayStay) / 256)
      }
      bool
    })

    //    （七）商务人员
    //    工作地在覆盖商务区的网络小区，年龄在18岁-70岁之间。工作地与居住地不在同一网络小区。在工作地时间为8点-20点范围内，在工作地但是不在该范围的占比小于50%。自费大于某门限（区别于附近的家庭主妇或老人）。
    val businessUser: RDD[(String, String, String, String, String, String, String)] = userLiveWorkPlace.filter(x => {
      var bool = false
      val businessMap: mutable.HashMap[String, ListBuffer[String]] = businessBroad.value
      var workPlace = ""
      var LivePlace = ""
      val work: String = x._5
      val live: String = x._6
      if (businessMap.exists(y => y._2.contains(work))) {
        workPlace = businessMap.filter(z => z._2.contains(work)).head._1
      }
      if (businessMap.exists(y => y._2.contains(live))) {
        LivePlace = businessMap.filter(z => z._2.contains(live)).head._1
      }

      var age: Int = 0
      var cost = 0
      val filter: List[(String, String, String, String, String)] = userAgeIncomeList.filter(_._1.equals(x._1))
      if (filter.nonEmpty) {
        age = NumberUtils.toInt(filter.head._4)
        cost = NumberUtils.toInt(filter.head._5)
      }

      (workPlace != "") && (age >= 18 && age <= 70) && ((NumberUtils.toInt(work) / 256) != NumberUtils.toInt(live) / 256) && cost > userBusinessIncome
    })

    val resultRDD: RDD[(String, String, String, String, String, String, String)] = educationUser.union(studentUser).union(doctorUser).union(businessUser)

    val path = new Path(outputPath)
    val fs = path.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(path)) {
      fs.delete(path, true)
      System.out.println(outputPath + "已存在，删除")
    }
    // 将这个结果先进行保存
    resultRDD.saveAsTextFile(outputPath)

    sc.stop()
    println("success!————人群标签识别模型" + date)
  }
}
























