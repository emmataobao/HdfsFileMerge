#!/bin/sh
source /etc/profile
export HADOOP_USER_NAME=cleandata
export home=/home/jp-spark/sparkApp/hdfsFileMerge
/opt/spark-2.0.1-bin-cdh5.4.4-without-hadoop/bin/spark-submit \
--master yarn \
--deploy-mode client \
--driver-memory 2G \
--executor-memory 4G \
--num-executors 24 \
--queue compass \
--executor-cores 2 \
--class com.surq.hdfs.file.merge.Main $home/lib/HdfsFileMerge-0.0.1-SNAPSHOT.jar \
--inFileType .gz \
--outFileType gz \
--limitFileSize 128 \
--input /cleandata/apps/cartoon/base/active/2019/02/02/offline \
--output /user/cleandata/output \
--ismove true \
--isdelete true \
--toEmail yangfengmei@moxiu.net >> ${home}/log/merge.log
