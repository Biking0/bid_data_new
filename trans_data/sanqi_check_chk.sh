#!/bin/bash
# ***************************************************************************
# 文件名称：sanqi_check_chk.sh
# 功能描述：检测大数据平台校验文件
# 输 入 表：source_table_name
# 输 出 表：target_table_name
# 创 建 者：wxq
# 创建日期：20190604
# 修改日志：hyn
# 修改日期：20200313
# ***************************************************************************
# 程序调用格式：sh trans_data_bdi2sanqi.sh source_table_name 20190604 target_table_name
# ***************************************************************************

# 执行环境以及用户认证
source /opt/fi_client/bigdata_env
kinit -kt /home/ocdp/user.keytab asiainfouser1

# 写日志
Write_Log_File()
{
    now_time=`date +"%Y-%m-%d %H:%M:%S"`
    echo "${now_time} ${1}"
    echo "${now_time} ${1}" >> ${Log_File}
}

# 传入参数处理，用于支持小时表、日表、月表
SOURCEDIR=$1
DATETIME=$2
TODIR=$3 
if [ ${#DATETIME} -eq 10 ]; then
  SHOWMONTH=${DATETIME:0:6} 
  SHOWDAY=${DATETIME:0:8} 
  SHOWHOUR=${DATETIME}  
  source_dir=${SOURCEDIR}/month_id=${SHOWMONTH}/day_id=${SHOWDAY}/hour_id=${SHOWHOUR}
  target_dir=${TODIR}/month_id=${SHOWMONTH}/day_id=${SHOWDAY}
  partition_dir="month_id=${SHOWMONTH},day_id=${SHOWDAY},hour_id=${SHOWHOUR}"
elif [ ${#DATETIME} -eq 8 ]; then
  SHOWMONTH=${DATETIME:0:6} 
  SHOWDAY=${DATETIME}
  source_dir=${SOURCEDIR}/month_id=${SHOWMONTH}/day_id=${SHOWDAY} 
  target_dir=${TODIR}/month_id=${SHOWMONTH}
  partition_dir="month_id=${SHOWMONTH},day_id=${SHOWDAY}"
elif [ ${#DATETIME} -eq 6 ]; then
  SHOWMONTH=${DATETIME}
  source_dir=${SOURCEDIR}/month_id=${SHOWMONTH} 
  target_dir=${TODIR}
  partition_dir="month_id=${SHOWMONTH}"
else 
  echo "datetime is not valid"
  exit 1
fi

# 测试数据加载参数

echo "source_dir: ${source_dir}"
echo "target_dir: ${target_dir}"
echo "to_dir: ${TODIR}"
echo "partition_dir:${partition_dir}"


#日志文件名可以自定义，这里用的是脚本名称+${DATETIME}.log
sh_name=`echo ${0} | awk -F "/" '{print $NF}' | awk -F "." '{print $1}'`
Log_Path="/hdfs/data01/trans"

Log_File="${Log_Path}/${sh_name}_${SOURCEDIR}_TO_${TODIR}_${DATETIME}.log"
echo ${Log_File}

#判断namenode存活主机ip：
hadoop fs -ls -d hdfs://10.218.59.8:25000/user/hive/warehouse/hwcdm.db/ &>/dev/null
if [ $? -eq 0 ]
then
    Write_Log_File "10.218.59.8 is active"
    DATA_FROM='10.218.59.8'
else
    Write_Log_File "10.218.59.7 is active"
    DATA_FROM='10.218.59.7'
fi

hadoop fs -ls -d hdfs://10.93.171.97:8020/user/hive/warehouse/ &>/dev/null
if [ $? -eq 0 ]
then
    Write_Log_File "10.93.171.97 is active"
    DATA_TO='10.93.171.97'
else
    Write_Log_File "10.93.171.100 is active"
    DATA_TO='10.93.171.100'
fi

while true
do
    # 判断数据是否准备就绪 ,读取通知文件是否存在 /asiainfo/dependent/tas_gprs_lte_app_20190601.txt
    file_num=`hadoop fs -ls  hdfs://$DATA_FROM:25000/asiainfo/dependent/${SOURCEDIR}_${DATETIME}.txt | wc -l`
    echo ${file_num}
    if [ ${file_num} -eq 1 ]
    then 
        echo 'ok'
		break
    else
        Write_Log_File "xxxxx未准备就绪，等待5分钟"
        sleep 300
    fi
done