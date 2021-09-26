package com.bonc.user_locate.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.{BufferedReader, InputStream, InputStreamReader}

object FileUtil {
  private val conf: Configuration = new Configuration()
//  private val fs: FileSystem = FileSystem.get(conf)

  def getInputStream(path: Path): InputStream = {
    val fileSystem = FileSystem.get(conf)
    fileSystem.open(path)
  }

  def getBufferedReader(path: Path): BufferedReader = {
    val inputStream = getInputStream(path)
    new BufferedReader(new InputStreamReader(inputStream))
  }
}
