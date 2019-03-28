package com.surq.hdfs.file.scan

import java.io.File
import scala.collection.mutable.ArrayBuffer
import java.io.FileWriter
import java.text.DecimalFormat

/**
 * 功能：
 * 1、递归遍历本地文件夹目录，按【文件名+大小】、【文件名】分组列出文件列表。
 * 2、递归遍历给定的目录src,des找出在des中存在（相同）的src文件。是否存在按文件名+大小】、【文件名】、【大小】三个维度衡量。
 */
object LocalMachineFileManager {

  // （名+长度的hashcode,名字，大小，路径）
  val jpgFileList_src = ArrayBuffer[(Int, String, Long, String)]()
  val jpgFileList_des = ArrayBuffer[(Int, String, Long, String)]()
  val df = new DecimalFormat("0.00")
  // 所有参数命令
  val Array(actionType, inputPath, srcPath, desPath, outputLog) = Array("actionType", "inputPath", "srcPath", "desPath", "outputLog")
  def main(args: Array[String]) {

    if (args(0).trim.toUpperCase.endsWith("HELP") || args(1).trim.toUpperCase.endsWith("HELP")) printArgInfo
    else {
      // 1、参数解析
      val paramMap = argAnalysis(args)
      val schema = paramMap.getOrElse("actionType", "0")
      val outputLogFile = paramMap.getOrElse("outputLog", "")
      val parm_inputPath = paramMap.getOrElse("inputPath", "")
      val parm_srcPath = paramMap.getOrElse("srcPath", "")
      val parm_desPath = paramMap.getOrElse("desPath", "")
      if (outputLogFile != "") {
        schema.trim match {
          // 列出inputPath文件列表,以文件名+文件大小分组列表输出
          case "1" => runPrintList(1)
          // 列出inputPath文件列表,以文件名分组列表输出
          case "2" => runPrintList(2)
          // 列出desPath中与srcPath文件名相同且大小也相等的文件列表输出
          case "3" => run(1)
          // 列出desPath中与srcPath文件名相同的文件列表输出
          case "4" => run(2)
          // 列出desPath中与srcPath文件大小相等的文件列表输出
          case "5" => run(3)
          case _   => printArgInfo
        }
      } else printArgInfo

      def runPrintList(selectId: Int) = if (parm_inputPath != "") printDirFilesList(parm_inputPath, outputLogFile, selectId) else printArgInfo
      def run(selectId: Int) = if (parm_srcPath != "" && parm_desPath != "") printSameFiles(parm_srcPath, parm_desPath, selectId, outputLogFile)
      else printArgInfo
    }
  }

  /**
   * 获取两个目录的相同文件列表（字名+大小）
   * src 中的照片文件在dec中相同文件列表
   * size_flg: true=文件名相同且大小也要相等； false：仅文件名相同
   */
  def printSameFiles(srcPath: String, desPath: String, schema: Int, outLog: String) =
    getFileBaseFun(getFilesInfoList, new File(srcPath), new File(desPath), schema, new FileWriter(outLog, false))

  /**
   * 递归遍历inputPath路径及子目录下的所有文件，并输出到outLog文件中
   */
  def printDirFilesList(inputPath: String, outLog: String, schema: Int) = {
    jpgFileList_src.clear
    val writer = new FileWriter(outLog, false)
    if (schema == 1) writer.write(s"-----列出[$inputPath]目录以及子目录中所有文件列表，并以【文件名+文件大小】为key 分组-----\n")
    else writer.write(s"-----列出[$inputPath]目录以及子目录中所有文件列表，并以【文件名】为key 分组-----\n")
    writer.write("相同文件个数" + "\t" + "路径列表" + "\n")
    getFilesInfoList(new File(inputPath), schema, jpgFileList_src)
    // reduceSameFile：（key,相同文件个数，名字，大小，路径）
    reduceSameFile(jpgFileList_src).foreach(line => {
      writer.write(line._2 + "\t" + line._5 + "\n")
      writer.flush
    })
    writer.close
  }
  //-----------------------------------------------------------------------------------------------------------

  /**
   * 回调函数查找文件
   * src中的key在des中存在的文件信息列表
   */
  def getFileBaseFun(selectFun: (File, Int, ArrayBuffer[(Int, String, Long, String)]) => Unit, src: File, des: File, schema: Int, writer: FileWriter) = {

    jpgFileList_src.clear
    jpgFileList_des.clear
    selectFun(src, schema, jpgFileList_src)
    selectFun(des, schema, jpgFileList_des)
    schema match {
      // 列出desPath中与srcPath文件名相同且大小也相等的文件列表
      case 1 => writer.write(s"-----列出["+des.getCanonicalPath+"]中与["+src.getCanonicalPath+"]文件名相同且大小也相等的文件列表-----\n")
      // 列出desPath中与srcPath文件名相同的文件列表
      case 2 => writer.write("-----列出["+des.getCanonicalPath+"]中与["+src.getCanonicalPath+"]文件名相同的文件列表-----\n")
      // 列出desPath中与srcPath文件大小相等的文件列表
      case 3 => writer.write("-----列出["+des.getCanonicalPath+"]中与["+src.getCanonicalPath+"]文件大小相等的文件列表-----\n")
    }
    writer.write("srcPath文件名" + "\t" + "srcPath文件大小（B）" + "\t" + "相同文件个数" + "\t" + "desPath相同文件路径列表" + "\n")
    writer.flush
    val dec_map = reduceSameFile(jpgFileList_des).map(line => (line._1, line._2 + "\t" + line._5)).toMap
    jpgFileList_src.map(line => {
      val value = dec_map.getOrElse(line._1, "")
      if (value != "") {
        writer.write(line._4 + "\t" + line._3 + "\t" + value + "\n")
        writer.flush
      }
    })
    writer.close()
  }

  /**
   * 相同hashcode 的文件分类
   * 把bufferList中的文件信息，相同hashcode的文件归为一条记录,路径用“，”隔开
   */
  def reduceSameFile(bufferList: ArrayBuffer[(Int, String, Long, String)]) =
    bufferList.map(line => (line._1, line)).groupBy(_._1).map(line => {
      val list = line._2
      val pathlist = list.map(info => info._2._4).mkString(",")
      // （key,相同文件个数，名字，大小，路径）
      (list(0)._2._1, list.size + "个相同", list(0)._2._2, list(0)._2._3, pathlist)
    }).toList

  /**
   * 获取文件列表信息并加入hashcode（名字+大小）
   *
   * 参数 schema: 1=文件名相同且大小也要相等； 2：文件名相同；3：文件大小
   *  schema= 1时，输出的第一列（key）为 名字+大小的hashcode;schema= 2时为 名字的hashcode;schema= 3时为文件大小的hashcode
   */
  def getFilesInfoList(file: File, schema: Int, list: ArrayBuffer[(Int, String, Long, String)]) {
    if (file.isDirectory) file.listFiles.map(f => getFilesInfoList(f, schema, list)) else {
      if (schema == 1) list += (((file.getName + file.length).hashCode, file.getName, file.length, file.getCanonicalPath))
      else if (schema == 2) list += ((file.getName.hashCode, file.getName, file.length, file.getCanonicalPath))
      else list += ((file.length.hashCode, file.getName, file.length, file.getCanonicalPath))
    }
  }

  /**
   * 当参数不对时的提示信息
   */
  def printArgInfo = {
    Console println "参数格式："
    Console println s"--$actionType ..........功能选项有四个值：【1|2|3|4|5】"
    Console println s" 				1:以[文件名+文件大小]为key，递归遍历$inputPath 路径及子目录下的所有文件，分组输出到 $outputLog 文件中。"
    Console println s" 				2:以[文件名]为key，递归遍历$inputPath 路径及子目录下的所有文件，分组输出到 $outputLog 文件中。"
    Console println s" 				3:列出$desPath 中与$srcPath 文件名相同且大小也相等的文件列表。"
    Console println s" 				4:列出$desPath 中与$srcPath 文件名相同的文件列表。"
    Console println s" 				5:列出$desPath 中与$srcPath 文件大小相等的文件列表。"
    Console println "		注意："
    Console println "		        $outputLog为必填项。"
    Console println "		        $actionType 为1、2时，$inputPath为必填项；$actionType为【3〜5】时$desPath、$srcPath为必填项"
    Console println s"--$inputPath  ........所在递归遍历的目录。"
    Console println s"--$srcPath .......想要检查的目录,检查$srcPath 中是否在$desPath 有重复文件。"
    Console println s"--$desPath ...............所对比的目录。"
    Console println s"--$outputLog ..............计算结果的输出文件。"

    Console println "=================================================================="
    Console println "【HDFS小文件合并】功能介绍："
    Console println "入口函数： com.surq.hdfs.file.scan.Self"
    Console println "在多目录、多层次存放文件时，会产生同一个文件或照片重复存放，最后导致一个文件多处保存。本工具的目的是找出相同的文件列表，进行手动改正。"

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
}