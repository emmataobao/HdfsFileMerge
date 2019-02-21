package com.surq.hdfs.file.merge

import java.io.File
import java.util.Properties
import java.io.FileInputStream

/**
 * @author 宿荣全
 * 寻找jar包绝对路径、config的路径、以及加载属性文件
 */
object LoadFilePath {
  /**
   * 获取jar包所在路径
   */
  val getJarPath = {
    val jarName = this.getClass.getProtectionDomain.getCodeSource.getLocation.getPath
    jarName.substring(0, jarName.lastIndexOf(System.getProperty("file.separator")))
  }
  /**
   * 获取config文件夹路径
   */
  val getConfigPath = getJarPath + "/../config/"
//  // TODO
//   val getConfigPath ="/moxiu/workspace/HdfsFileMerge/config/"

  /**
   * 指定config/文件夹下的配置文件名，默认会加载config下的所有.properties的文件
   */
  def loadProperties(fileName: String = "*.properties") = {
    val properties = new Properties
    if (fileName == "*.properties") new File(getConfigPath).listFiles.foreach(f => properties.load(new FileInputStream(f.getAbsolutePath)))
    else properties.load(new FileInputStream(getConfigPath + fileName))
    properties
  }
}