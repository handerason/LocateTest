package com.bonc.user_locate

import org.apache.spark.Partitioner

/**
  * 自定义分区类，按随机数重分区，按eci排序
  */
class MyPartitioner(partitions: Int) extends Partitioner {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[String]
    k.split("_")(0).toInt.hashCode() % numPartitions
  }
}