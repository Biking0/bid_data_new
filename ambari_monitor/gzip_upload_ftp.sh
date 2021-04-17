#!/bin/bash
# gzip_upload_ftp.sh
# hyn
# 20210416

# 文件打包为gz，并上传到ftp服务器
# 172.19.167.13

file_name=$1
path='/data/ocdp/dataos/166_DAY_TB_DM_CU_WLTX_HLJ_COMPANY_BASIC_DAY_01'

# 压缩文件
gzip -c $path/$file_name > $path/$file_name.gz

# 上传文件
lftp << EOF
open ftp://nglocal:yuSC2!90@172.20.16.22
cd /data/nglocal/visitorData/
mput $path/$file_name.gz
close
bye
EOF







