package com.surq.hdfs.file.scan

import com.surq.hdfs.file.merge.LoadFilePath
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import scala.collection.mutable.ArrayBuffer
import com.surq.hdfs.file.merge.Main

object Scan {
  // 所有参数命令
  val Array(limitFileSize, input, delEmptyFileFlg) = Array("limitFileSize", "input", "delEmptyFileFlg")

  def main(args: Array[String]) {
    if (args(0).trim.toUpperCase.endsWith("HELP") || args(1).trim.toUpperCase.endsWith("HELP")) printArgInfo
    else {
      require(args.size == 3 * 2, printArgInfo)
      // 1、参数解析
      val actionType = Main.argAnalysis(args)
      val inputPath = new Path(actionType("input"))
      val fileLimit = actionType("limitFileSize").toInt
      val delEmptyFileFlg = actionType("delEmptyFileFlg").toBoolean

      // 2、获取HDFS文件列表
      val HDFSFileSytem = getHDFSFileSytem
      val folderList = ArrayBuffer[String]()
      scanDir(inputPath)
      folderList foreach println
      /**
       * 递归遍历目录
       */
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
                // 去除_SUCCESS文件后，如果没有文件，则删除此目录，否则记录此目录
                val fileCount = listStatus.map(file => {
                  val fileSize = file.getLen
                  // 去除_SUCCESS文件后
                  if ((file.getPath.getName) != "_SUCCESS") {
                    // 删除无效的0字节文件
                    if (fileSize == 0l && delEmptyFileFlg) {
                      HDFSFileSytem.delete(f.getPath, false)
                      Console println "delete empty File: " + f.getPath
                      None
                      // 对大于120M的文件不进行合并
                    } else if (fileSize >= 1024l * 1024 * fileLimit) None else Some(f)
                  } else None

                }).filter(_ != None)
                // 1个文件以上才会合并
                if (fileCount.size > 1) folderList += f.getPath.toString
              }
            }
          })
        } catch {
          case e: Exception => e.printStackTrace()
        }
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

  /**
   * 当参数不对时的提示信息
   */
  def printArgInfo = {
    Console println "参数格式："
    Console println s"--$input ...............扫描路径，递归扫描。"
    Console println s"--$delEmptyFileFlg ...............true:删除空文件（忽略【_SUCCESS】）。"
    Console println s"--$limitFileSize ..............如果一个文件夹中存在两个小于此值的文件，文件目录将被列出。单位（M）"
    Console println "=================================================================="
    Console println "功能："
    Console println "入口函数：com.surq.hdfs.file.scan.Scan"
    Console println "递归遍历指定的input路径,找出存放文件的最后一级文件夹目录。在遍历过程中将删除0字节的文件（忽略【_SUCCESS】）。"
    Console println "文件夹中存在多于一个小于limitFileSize值的文件，此目录将被列出。"
  }
}