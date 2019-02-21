package com.surq.hdfs.file.merge

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.compress.GzipCodec
import scala.collection.mutable.ArrayBuffer

object Main {
  // 所有参数命令
  val Array(inFileType, outFileType, limitFileSize, input, output, ismove, isdelete, toEmail) = Array("inFileType", "outFileType", "limitFileSize", "input", "output", "ismove", "isdelete", "toEmail")
  def main(args: Array[String]) {
    require(args.size == 8 * 2, printArgInfo)

    // 1、参数解析
    val actionType = argAnalysis(args)
    Console println "========>文件合并功能启动<========参数一览表:"
    actionType.map(kv => println(kv._1 + ":" + kv._2))

    val outType = actionType(outFileType).trim
    val inType = actionType(inFileType).trim
    val inputPath = actionType(input).trim
    val outputPath = actionType(output).trim
    //异常邮件信息
    val mesgList = ArrayBuffer[(String, String)]()

    // 2、获取HDFS文件列表
    val HDFSFileSytem = getHDFSFileSytem
    val fileListInfo = getFileList(HDFSFileSytem, inputPath, inType)
    val fileList = fileListInfo.map(_._1)
    val tatolSize = fileListInfo.map(_._2).sum

    // 3、spark重新切分文件
    val conf = new SparkConf().setAppName("hdfs.file.merge")
    val sc = new SparkContext(conf)
    // 不压缩输出文件内容大小限定
    val PER_FILE_SIZE = 1024d * 1024 * actionType(limitFileSize).toInt
    //压缩输出文件内容大小限定 128M gzip保守压缩率1/4.5
    val PER_GZIP_FILE_SIZE = PER_FILE_SIZE * 4.5
    // 计算输出文件的个数
    val partNum = if (outType.toLowerCase.endsWith("gz")) Math.ceil(tatolSize / PER_GZIP_FILE_SIZE) else Math.ceil(tatolSize / PER_FILE_SIZE)
    val fileRDD = sc.textFile(fileList.mkString(",")).repartition(partNum.toInt)
    if (outType.toLowerCase.endsWith("gz")) fileRDD.saveAsTextFile(outputPath, classOf[GzipCodec]) else fileRDD.saveAsTextFile(outputPath)
    sc.stop

    // 4、检验spark 程序运行是否成功
    val successFile = new Path(outputPath + System.getProperty("file.separator") + "_SUCCESS")
    if (HDFSFileSytem.exists(successFile)) {
      Console println "------spark合并文件[" + actionType(input) + "]执行成功。"
      // 5、文件移动功能
      if (actionType(ismove).trim.toBoolean) {
        Console println "------合并结果文件夹[" + actionType(output) + "]下的文件正在向[" ++ actionType(input) + "]转移。"
        // 合并后的新文件
        val srcFileList = getFileList(HDFSFileSytem, outputPath, "all").filter(!_._1.endsWith("_SUCCESS")).map(f => f._1)
        val srcFileNameList = srcFileList.map(f => f.substring(f.lastIndexOf(System.getProperty("file.separator")) + 1)).filter(_ != "_SUCCESS")
        // 移动到输入数据的文件夹
        val dstFileList = srcFileNameList.map(f => inputPath + System.getProperty("file.separator") + f)
        // 移动
        for (file <- srcFileList.zip(dstFileList)) {
          try {
            HDFSFileSytem.rename(new Path(file._1), new Path(file._2))
          } catch {
            case e: Exception => mesgList += (("文件移动功能", "合并文件:" + file._1 + "移动到" + file._2 + "移动失败。"))
          }
        }
        // 只有_SUCCESS文件说明全部移完
        if (HDFSFileSytem.listStatus(new Path(outputPath)).size <= 1) HDFSFileSytem.delete(new Path(outputPath), true)
      }
      // 6、原始数据文件删除功能
      if (actionType(isdelete).trim.toBoolean) {
        Console println "------正在删除[" + actionType(input) + "]下的合并数据源文件。"
        //递归删除：false
        for (file <- fileList) {
          try {
            HDFSFileSytem.delete(new Path(file), false)
          } catch {
            case e: Exception => mesgList += (("文件删除功能", "文件:" + file + "删除失败。"))
          }
        }
      }
      Console println "------[" + actionType(input) + "]的合并、(是否)转移、(是否)删除源数据全部运行完成。"
    } else {
      // spark 程序运行失败
      mesgList += (("文件合并功能", inputPath + "下的" + inType + "类型合并spark 运行失败。"))
    }
    // 7、发送错误信息
    val addressList = actionType(toEmail).trim.split(",")
    if (addressList.size > 0 && addressList(0).trim != "none" && mesgList.size > 0) {
      // 发邮件
      val msg = mesgList.map(line => line._1 + ":\t" + line._2).mkString("<p>", "</p><p>", "</p>")
      MailUtil.sendHtmlMail(addressList, "HDFS文件合并报警", msg)
    }
  }

  /**
   * 参数解析
   */
  def argAnalysis(args: Array[String]) = {
    args.map(_.trim).filter(_ != "").mkString("#").split("--").filter(_ != "").map(param => {
      val par_param = param.split("#")
      val key = par_param(0).trim
      val paramValue = par_param(1).trim
      (key, paramValue)
    }).toMap
  }

  /**
   * 当参数不对时的提示信息
   */
  def printArgInfo = {
    Console println "参数格式："
    Console println s"--$inFileType ..........欲合并文件的类型，支持文本文件和gzip压缩格式的文件,all：压缩input的下的所有文件。"
    Console println s"--$outFileType  ........合并结果文件的类型,gz:压缩格式;txt:文本格式(除gz之外所有参数全部认为是文本格式)。"
    Console println s"--$limitFileSize .......压缩结果文件的上限大小,单位M比如：128"
    Console println s"--$input ...............欲合并文件的路径。"
    Console println s"--$output ..............合并结果的输出路径。"
    Console println s"--$ismove...............是否把生成的结果移动到输入路径（input）中，设为true时转移完成后会将output路径删除。"
    Console println s"--$isdelete.............合并成功后是否把原始数据(input下的欲合并文件)删除。"
    Console println s"--$toEmail .............报警邮件接收人地址用逗号隔开,设为none将不发送邮件。"
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
   * 计算需要合并的文件列表,以及文件大小
   */
  def getFileList(hdfs: FileSystem, dir: String, fileType: String) =
    if (fileType.trim == "all") hdfs.listStatus(new Path(dir)).map(f => (f.getPath.toString, f.getLen))
    else hdfs.listStatus(new Path(dir)).filter(f => f.getPath.toString.endsWith(fileType.trim)).map(f => (f.getPath.toString, f.getLen))
}