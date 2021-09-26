package com.bonc.user_locate.utils

import java.io.{BufferedReader, IOException, InputStream, InputStreamReader}
import java.util

import mrLocateV2.bsparam.{BsParam, Cell, EarfcnPciUnitedKey}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ListBuffer


object LocateUtils {

  // 检测目录或文件是否存在
  def checkFile(outputPath: String, idDeleted: Boolean, sc: SparkContext): Boolean = {
    var flag = false
    val path = new Path(outputPath)
    val fs = path.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(path)) {
      flag = true
      if (idDeleted) {
        fs.delete(path, true)
        System.out.println(outputPath+"已存在，删除")
      }
    }
    flag
  }

  // 装载工参
  def readBs2Cache(sc: SparkContext, isLocal: String, bsPath: String, cellMap: java.util.Map[java.lang.Long, Cell], eciMaps: util.Map[Integer, util.HashMap[EarfcnPciUnitedKey, java.lang.Long]]): Unit = {
    val isExisted = checkFile(bsPath, false, sc)
    if (!isExisted) {
      System.out.println("工参路径不存在,请检查配置文件......")
      System.exit(1)
    }
    val path = new Path(bsPath)
    if ("0".equals(isLocal)) { //集群模式
      var bufferedReader:BufferedReader = null
      var inputStream: InputStream = null
      var lineTxt:String = null
      try {
        val bsParam = new BsParam
        val fs = FileSystem.get(sc.hadoopConfiguration)
        inputStream = fs.open(new Path(bsPath)).getWrappedStream()
        //new InputStreamReader(inputStream)
        bufferedReader = new BufferedReader(new InputStreamReader(inputStream))
        //inputStream.read
        while ((lineTxt = bufferedReader.readLine()) != null) {
          bsParam.stringTo(lineTxt)
          // 装载到map
          cellMap.put(bsParam.getServerCell.getEci, bsParam.getServerCell.clone.asInstanceOf[Cell])
          eciMaps.put(bsParam.getServerCell.getEnodebId, bsParam.getEciMap.clone.asInstanceOf[util.HashMap[EarfcnPciUnitedKey, java.lang.Long]])
        }
      } catch {
        case e: Exception => //e.printStackTrace()
      } finally if (bufferedReader != null)
        try
          bufferedReader.close()
        catch {
          case e: IOException => e.printStackTrace()
        }
    }
    else {  //本地模式
      BsParam.readStrBsParam2Cache(path.toString, cellMap, eciMaps)
    }
  }

  // 输入路径处理
//  def addInputPath(inputs: ListBuffer[String], input: String, date: String, sc: SparkContext): Unit = {
//    val mrType = sc.hadoopConfiguration.get("mr.type")
//    if ("1".equals(mrType) || "2".equals(mrType)) {
//      if (!"".equals(input)) {
//        var in = input
//        if (!date.isEmpty) {
//          in = in + date
//        }
//        val isExisted = checkFile(in, false, sc)
//        if (isExisted) {
//          inputs.append(in)
//        }
//      }
//    } else {
//      if (!"".equals(input)) {
//        var in = input
//        if (!date.isEmpty) {
//          in = in + "'" + date + "'"
//        }
//        inputs.append(in)
//      }
//    }
//  }

  // 输入路径处理
  def addOTTInputPath(inputs: ListBuffer[String], input: String, date: String, sc: SparkContext): Unit = {
    val mrType = sc.hadoopConfiguration.get("ottmr_type")
    val proid = sc.hadoopConfiguration.get("proid")
    if ("1".equals(mrType)) {
      if (!"".equals(input)) {
        var inputpath = proid match {
          case "0" => {
            var in = input
            if (!date.isEmpty) {
              // /user/yingyong/mrott_data/20190418
              if (in.endsWith("/")) {
                in = in.substring(0, in.length - 1)
              }
              in = in + "/" + date
            }
            in
          }
          case "10" => {
            var in = input
            if (!date.isEmpty) {
              if (in.endsWith("/")) {
                in = in.substring(0, in.length - 1)
              }
              val day = date.substring(2,8)
              in = in + day
            }
            in
          }
        }
        println("ottpath = " + inputpath)
        val isExisted = checkFile(inputpath, false, sc)
        if (isExisted) {
          if (proid.equals("0")){
            inputpath = inputpath + "/*"
          }
          inputs.append(inputpath)
        }
      }
    }
//    else {
//      if (!"".equals(input)) {
//        var in = input
//        if (!date.isEmpty) {
//          in = in + "'" + date + "'"
//        }
//        inputs.append(in)
//      }
//    }
  }

}