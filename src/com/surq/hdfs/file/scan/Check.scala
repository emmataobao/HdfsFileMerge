package com.surq.hdfs.file.scan

import com.surq.hdfs.file.merge.LoadFilePath
import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import com.surq.hdfs.file.merge.MailUtil

/**
 * check合并完成有，有哪些合并路径没有数据【.gz】打印输出。
 */
object Check {
  def main(args: Array[String]) {
    val HDFSFileSytem = getHDFSFileSytem
    val folderList = ArrayBuffer[String]()
    folderList += args(0)
    scanDir(new Path(args(0)))
    // 发送邮件
    folderList += "共计:" + (folderList.size - 1) + "个文件夹."
    MailUtil.sendHtmlMail(args(1).split(","), "HDFS文件合并有误文件夹", folderList.mkString("<p>", "</p><p>", "</p>"))
    Console println "-----数据文件夹列表start----"
    folderList foreach println
    Console println "-------------end------------"
    def scanDir(hdfsPath: Path) {
      try {
        val status = HDFSFileSytem.listStatus(hdfsPath)
        status.foreach(f => {
          if (f.isDirectory) {
            val listStatus = HDFSFileSytem.listStatus(f.getPath)
            // 大于0说明还有下级目录
            val lastFolder_flg = (listStatus.map(f => if (f.isDirectory) 1 else 0)).sum
            if (lastFolder_flg > 0) scanDir(f.getPath)
            else {
              val fileCount = listStatus.map(_.getPath.toString).filter(_.endsWith("_SUCCESS"))
              if (listStatus.size == 0) folderList += f.getPath.toString()
            }
          }
        })
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }
  /**
   * 获取hadoop 文件系统
   */
  def getHDFSFileSytem = {
    // HDFS 文件配置
    val configPath = LoadFilePath.getConfigPath
    val conf = new Configuration
    //否则报：No FileSystem for scheme: hdfs
    conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
    conf.addResource(new Path(configPath + "core-site.xml"))
    conf.addResource(new Path(configPath + "hdfs-site.xml"))
    FileSystem.get(conf)
  }
}