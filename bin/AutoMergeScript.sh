#!/bin/sh

dirArray=("/cleandata/web/nginx/mobile/mobile.moxiu.com/mobile/2017/01/01" /
"/cleandata/web/nginx/mobile/mobile.moxiu.com/mobile/2017/01/02" /
"/cleandata/web/nginx/mobile/mobile.moxiu.com/mobile/2017/01/05" /
"/cleandata/web/nginx/mobile/mobile.moxiu.com/mobile/2017/01/06" /
"/cleandata/web/nginx/mobile/mobile.moxiu.com/mobile/2017/01/07" /
"/cleandata/web/nginx/mobile/mobile.moxiu.com/mobile/2017/12/29")

for subpath in "${dirArray[@]}"
do
sh hdfsFileMergeTool_demo_surq.sh $1 $subpath
echo $subpath;
done;