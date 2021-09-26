package com.bonc.user_locate

import com.bonc.lla.LLA
import com.bonc.location.LocType
import com.bonc.data.Record
import org.apache.commons.lang.math.NumberUtils
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}
import com.bonc.trace.Trace

import scala.collection.mutable

object user_trace {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("com").setLevel(Level.WARN)

    if (args.length < 2) {
      println("args must be at least 2")
      System.exit(1)
    }
    val configPath = args(0)  // 配置文件路径，与jar包相同目录
    val date = args(1)    // 日期
    val outputPath = args(2)

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

    val sql = "select imsi,locType,stime,mainCellId,longitude,latitude from "+database+".user_locate_res_history where day_id="+date+" and hour=23 and locType=7 || locType="
     val resultRdd =  hc.sql(sql).rdd.map(line => {
      val imsi = line.getString(0)
      val locType = line.getString(1)
      val stime = NumberUtils.toLong(line.getString(2))
      val mainCellid = NumberUtils.toLong(line.getString(3))
      val longitude = NumberUtils.toDouble(line.getString(4))
      val latitude = NumberUtils.toDouble(line.getString(5))

      val record = new Record
      record.lLA = new LLA(longitude,latitude)
      record.sTime = stime
      record.eci = mainCellid
      record.imsi = imsi
      (imsi,record)
    }).groupByKey().map(x =>{
        val recordList = x._2.toList
        val sortRecordList = recordList.sortBy(_.sTime)
        val trace = new Trace(LocType.SECTOR)
        trace.create(x._1,sortRecordList)
        val wheres = trace.wheres
        val sb: StringBuilder = new mutable.StringBuilder()
        wheres.foreach(where => {
         sb.append(where)
         sb.append("#")
       })
       if(sb.length>1 && sb.contains("#")){
         sb.substring(0,sb.length-1)
       }
       //(com.bonc.trace.imsi,wheres.size)
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
