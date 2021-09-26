package com.bonc.test

import org.apache.spark.{SparkConf, SparkContext}

class Student {

}

//创建key类，key组合键为grade，score
case class StudentKey(grade:String, score:Int)
//  extends Ordered[StudentKey]{
//  def compare(that: StudentKey) : Int = {
//    var result:Int = this.grade.compareTo(that.grade)
//    if (result == 0){
//      result = this.student.compareTo(that.student)
//      if(result ==0){
//        result = that.score.compareTo(this.score)
//      }
//    }
//    result
//  }
//}

object StudentKey {
  implicit def orderingByGradeStudentScore[A <: StudentKey] : Ordering[A] = {
    //    Ordering.by(fk => (fk.grade, fk.student, fk.score * -1))
    Ordering.by(fk => (fk.grade, fk.score * -1))
  }
}

object Student{
  def main(args: Array[String]) {

    //定义hdfs文件索引值
    val grade_idx:Int=0
    val student_idx:Int=1
    val course_idx:Int=2
    val score_idx:Int=3

    //定义转化函数，不能转化为Int类型的，给默认值0
    def safeInt(s: String): Int = try { s.toInt } catch { case _: Throwable  => 0 }

    //定义提取key的函数
    def createKey(data: Array[String]):StudentKey={
      StudentKey(data(grade_idx),safeInt(data(score_idx)))
    }

    //定义提取value的函数
    def listData(data: Array[String]):List[String]={
      List(data(grade_idx),data(student_idx),data(course_idx),data(score_idx))
    }

    def createKeyValueTuple(data: Array[String]) :(StudentKey,List[String]) = {
      (createKey(data),listData(data))
    }

    //创建分区类
    import org.apache.spark.Partitioner
    class StudentPartitioner(partitions: Int) extends Partitioner {
      require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

      override def numPartitions: Int = partitions

      override def getPartition(key: Any): Int = {
        val k = key.asInstanceOf[StudentKey]
        k.grade.hashCode() % numPartitions
      }
    }

    //设置master为local，用来进行本地调试
    val conf = new SparkConf().setAppName("Student_partition_sort").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //学生信息是打乱的
    val student_array = Array(
      "c001,n003,chinese,59",
      "c002,n004,english,79",
      "c002,n004,chinese,13",
      "c001,n001,english,88",
      "c001,n002,chinese,10",
      "c002,n006,chinese,29",
      "c001,n001,chinese,54",
      "c001,n002,english,32",
      "c001,n003,english,43",
      "c002,n005,english,80",
      "c002,n005,chinese,48",
      "c002,n006,english,69"
    )
    val student_rdd = sc.parallelize(student_array)
    val student_rdd2 = student_rdd.map(line => line.split(",")).map(createKeyValueTuple)
    val student_rdd3 = student_rdd2.repartitionAndSortWithinPartitions(new StudentPartitioner(10))
    student_rdd3.collect.foreach(println)
  }
}
