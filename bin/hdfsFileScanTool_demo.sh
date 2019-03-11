#!/bin/sh
## $1: hadoop 用户
## ¥2: 要搜索的根目录
## 指定一个目录，扫描该目录下所有终结子目录，打印需要合并的终结子目录
source /etc/profile
export HADOOP_USER_NAME=$1
objHome=/home/jp-spark/sparkApp/hdfsFileMerge
java -cp $objHome/lib/HdfsFileMerge-0.0.1-SNAPSHOT.jar com.surq.hdfs.file.scan.Scan \
--limitFileSize 120 \
--delEmptyFileFlg false \
--input $2 > $objHome/log/scan_new.log