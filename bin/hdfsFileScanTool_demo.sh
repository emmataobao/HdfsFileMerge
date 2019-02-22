#!/bin/sh
## $1: hadoop 用户
## ¥2: 要搜索的根目录
source /etc/profile
export HADOOP_USER_NAME=$1
objHome=/home/jp-spark/sparkApp/hdfsFileCheckTool
java -cp $objHome/lib/HdfsFilescan.jar com.surq.hdfs.file.scan.Scan \
--limitFileSize 120 \
--delEmptyFileFlg true \
--input $2 > $objHome/log/scan.log