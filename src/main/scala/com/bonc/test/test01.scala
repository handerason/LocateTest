package com.bonc.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object test01 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("com").setLevel(Level.WARN)

    if (args.length < 2) {
      println("args must be at least 2")
      System.exit(1)
    }
    val configPath = args(0)  // 配置文件路径，与jar包相同目录
    val date = args(1)    // 日期

    val conf = new SparkConf()



    val sc = new SparkContext(conf) // 创建SparkContext
    val hc = new HiveContext(sc)

    val seq: List[(String, String)] = List(("1236546","fsdfsdfsf"),
      ("1236546","fsdfsdfsf"),
      ("1236546","fsdfsdfsf"),
      ("1236546","fsdfsdfsf"),
      ("1236546","fsdfsdfsf"),
      ("1236546","fsdfsdfsf"),
      ("1236546","fsdfsdfsf"),
      ("1236546","fsdfsdfsf"))
    val resultRdd = sc.makeRDD(seq)

    val schema = "imsi wheres"
    val scheme_struct: StructType = StructType(schema.split(" ").map(field_name => StructField(field_name, StringType, true)))
    val rowRdd: RDD[Row] = resultRdd.map(x => (Row(x._1.toString,x._2.toString)))
    val resDataFrame:DataFrame = hc.createDataFrame(rowRdd,scheme_struct)
    resDataFrame.registerTempTable("temp_res_table")

    var partition = "day_id='" + date+"'"
    val insertsql = "insert overwrite table " + "dfgx" + ".tb_user_trace partition(" + partition + ") " +
      " select imsi,wheres from temp_res_table"
    println("-----insert-sql-----:"+insertsql)
    hc.sql(insertsql)
    sc.stop()
    println("success!————生成轨迹" + date)

  }

}
