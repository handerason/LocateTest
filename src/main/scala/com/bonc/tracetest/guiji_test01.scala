package com.bonc.tracetest

import java.text.SimpleDateFormat
import java.util
import java.util.TimeZone
import com.bonc.lla.LLA
import com.bonc.location.LocType
import com.bonc.user_locate.utils.LocateUtils
import mrLocateV2.bsparam.{Cell, EarfcnPciUnitedKey}
import org.apache.commons.lang.math.NumberUtils
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import com.bonc.trace.{Trace, Where}
import com.bonc.user_locate.ReadParam

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object guiji_test01 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("com").setLevel(Level.WARN)

//    if (args.length < 2) {
//      println("args must be at least 2")
//      System.exit(1)
//    }
    val configPath = args(0)  // 配置文件路径，与jar包相同目录
    val date = args(1)    // 日期

    val conf = new SparkConf()
    ReadParam.readXML(conf,configPath)
    conf.set("spark.sql.shuffle.partitions", "1000")
    conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    conf.set("date",date)
    conf.set("spark.yarn.queue", conf.get("queuename"))
    conf.setAppName("user_trace"+date)
    conf.set("spark.driver.maxResultSize", "20g")
    // 使用kryo序列化
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array[Class[_]](classOf[Nothing]))

    //判断是否启用本地模式
    val isLocal = conf.get("local")
    if ("1".equals(isLocal)) {
      conf.setMaster("local[*]")
    }
    val isTest = conf.getBoolean("isTest", false)
    val isMainCellFilter = conf.getBoolean("mr_isMainCellFilter", false)
    val database = conf.get("database","default")

    val sc = new SparkContext(conf) // 创建SparkContext
    val hc = new HiveContext(sc)    // 创建HiveContext

//    var outputPath = conf.get("outputpath")
//    var inputpath = conf.get("inputpath")
    var inputpath = "E:\\Work\\位置平台产品测试\\测试数据\\test01.txt"
    var outputPath = "E:\\Work\\位置平台产品测试\\测试数据\\tracetest01"

    val cellMap = new util.HashMap[java.lang.Long, Cell]()
    val eciMap = new util.HashMap[Integer, util.HashMap[EarfcnPciUnitedKey, java.lang.Long]]
    LocateUtils.readBs2Cache(sc, isLocal, conf.get("site", ""), cellMap, eciMap)
    System.out.println("cellMap---" + cellMap.size())
    System.out.println("eciMap---" + eciMap.size())
    // 1.2将工参信息广播
    val bd_cellMap = sc.broadcast(cellMap)
    val bd_eciMap = sc.broadcast(eciMap)

    val resultRdd = sc.textFile(inputpath).mapPartitions(iter =>{
      val list = new ListBuffer[(String,Where)]
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      format.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
      var mapCellMap = bd_cellMap.value
      while (iter.hasNext){
        val line = iter.next().toString
        var arr = line.split("\t")
        val date = arr(0)
        val phione = arr(1)
        val eci = NumberUtils.toLong(arr(2))
        val imsi = arr(3)
        val cell = mapCellMap.get(eci)
        if(cell!=null && cell.getXYZ!=null){
          val longitude = cell.getXYZ.toLB(105).getLongtitude
          val latitude = cell.getXYZ.toLB(105).getLatitude
          val starttime = format.parse(arr(5)).getTime
          val endtime = format.parse(arr(7)).getTime

          val where = new Where
          where.lLA = new LLA(longitude,latitude)
          where.sTime = starttime
          where.eTime = endtime
          where.code=eci.toString
          list.append((phione,where))
        }
      }
      list.toIterator
    }).groupByKey().map(x =>{
      val recordList = x._2.toList
      val sortRecordList = recordList.sortBy(_.sTime).sortBy(_.eTime)
      val trace = new Trace(LocType.SECTOR)
      trace.imsi = x._1
      trace.date = date
      trace.createByWhere(sortRecordList)
      val wheres = trace.wheres
      val sb: StringBuilder = new mutable.StringBuilder()
      wheres.foreach(where => {
        sb.append(where)
        sb.append("#")
      })
      if(sb.length>1 && sb.contains("#")){
        sb.substring(0,sb.length-1)
      }
      (trace.imsi,sb.toString())
    })

    val path = new Path(outputPath)
    val fs = path.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(path)) {
      fs.delete(path, true)
      System.out.println(outputPath + "已存在，删除")
    }
    resultRdd.saveAsTextFile(outputPath)

//    val schema = "imsi wheres"
//    val scheme_struct = StructType(schema.split(" ").map(field_name => StructField(field_name, StringType, true)))
//    val rowRdd = resultRdd.map(x => (Row(x._1.toString,x._2.toString)))
//    val resDataFrame:DataFrame = hc.createDataFrame(rowRdd,scheme_struct)
//    resDataFrame.registerTempTable("temp_res_table")
//
//    var partition = "day_id=" + date
//    val insertsql = "insert overwrite table " + database + ".tb_user_trace partition(" + partition + ") " +
//      " select imsi,wheres from temp_res_table"
//    println("-----insert-sql-----:"+insertsql)
//    hc.sql(insertsql)
    sc.stop()
    println("success!————生成轨迹" + date)
  }


}
