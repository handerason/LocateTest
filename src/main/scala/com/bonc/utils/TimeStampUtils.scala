package com.bonc.utils

import java.text.SimpleDateFormat

/**
 * @Author: whzHander 
 * @Date: 2021/8/20 17:49 
 * @Description:
 * This code sucks, you know it and I know it.
 * Move on and call me an idiot later.
 * If this code works, it was written by Wang Hengze.
 * If not, I don't know who wrote it.
 */
object TimeStampUtils {
  def tranTimeToLong(tm: String): Long = {
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dt = fm.parse(tm)
    val tim: Long = dt.getTime
    tim
  }


  def longToStringMinute(tm: Long): String = {
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm")
    val lm = fm.format(tm)
    lm
  }

  def longToTranTime(tm: Long): String = {
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val lm = fm.format(tm)
    lm
  }
}
