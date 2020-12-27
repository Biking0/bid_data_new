#!/bin/bash
# ***************************************************************************
# 文件名称：trans_data_suyan2ocdp.sh
# 功能描述：大数据平台hdfs数据同步到三期hdfs集群，包括月表、日表、小时表
# 1.大数据平台hdfs数据同步到三期hdfs集群
# 2.支持月表、日表、小时表
# 3.hdfs数据文件拷贝完成后，并加载数据到hive库，自动添加分区
# 4.注：若目标集群表分区已存在，需要先删除分区，否则加载失败
# 5.删除分区：alter table dw_ay_spots_user_yyyymmdd drop partition(day_id=20191214);
# 输 入 表：source_table_name
# 输 出 表：target_table_name
# 创 建 者：hyn
# 创建日期：20200618
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：sh trans_data_suyan2ocdp.sh source_table_name 20190604 target_table_name
# ***************************************************************************

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


# 小时表
if [ ${#DATETIME} -eq 10 ]; then
  SHOWMONTH=${DATETIME:0:6}
  SHOWDAY=${DATETIME:0:8}
  SHOWHOUR=${DATETIME}
  source_dir=${SOURCEDIR}/month_id=${SHOWMONTH}/day_id=${SHOWDAY}/hour_id=${SHOWHOUR}
  target_dir=${TODIR}/month_id=${SHOWMONTH}/day_id=${SHOWDAY}

  # 添加分区，用于加载数据
  partition_dir="month_id=${SHOWMONTH},day_id=${SHOWDAY},hour_id=${SHOWHOUR}"

# 日表
elif [ ${#DATETIME} -eq 8 ]; then
  SHOWMONTH=${DATETIME:0:6}
  SHOWDAY=${DATETIME}

  source_dir=${SOURCEDIR}/month_id=${SHOWMONTH}/day_id=${SHOWDAY}
  target_dir=${TODIR}/month_id=${SHOWMONTH}

  # 添加分区，用于加载数据
  partition_dir="month_id=${SHOWMONTH},day_id=${SHOWDAY}"

# 月表
elif [ ${#DATETIME} -eq 6 ]; then
  SHOWMONTH=${DATETIME}
  source_dir=${SOURCEDIR}/month_id=${SHOWMONTH}
  target_dir=${TODIR}

  # 添加分区，用于加载数据
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
Log_Path="./log"

Log_File="${Log_Path}/${sh_name}_${SOURCEDIR}_TO_${TODIR}_${DATETIME}.log"
echo ${Log_File}

#判断namenode存活主机IP
hadoop fs -ls -d hdfs://192.168.190.89:8020/apps/hive/warehouse/csap.db/ &>/dev/null
if [ $? -eq 0 ]
then
    Write_Log_File "192.168.190.89 is active"
    DATA_FROM='192.168.190.89'
else
    Write_Log_File "10.218.59.7 is active"
    DATA_FROM='10.218.59.7'
fi

hadoop fs -ls -d hdfs://172.19.168.4:8020/warehouse/tablespace/managed/hive &>/dev/null
if [ $? -eq 0 ]
then
    Write_Log_File "172.19.168.4 is active"
    DATA_TO='172.19.168.4'
else
    Write_Log_File "10.93.171.100 is active"
    DATA_TO='10.93.171.100'
fi

# 创建HDFS目标表目录
        Write_Log_File "hadoop fs -mkdir -p hdfs://$DATA_TO:8020/warehouse/tablespace/managed/hive/${target_dir} >> ${Log_File}"
        hadoop fs -mkdir -p hdfs://$DATA_TO:8020/warehouse/tablespace/managed/hive/${target_dir} >> ${Log_File}

		# 开始拷贝数据
        Write_Log_File "hadoop distcp -i hdfs://$DATA_FROM:8020/apps/hive/warehouse/csap.db/${source_dir}  hdfs://$DATA_TO:8020/warehouse/tablespace/managed/hive/${target_dir} >> ${Log_File}"
        hadoop distcp -i hdfs://$DATA_FROM:25000/apps/hive/warehouse/csap.db/${source_dir}  hdfs://$DATA_TO:8020/warehouse/tablespace/managed/hive/${target_dir} >> ${Log_File}



		# 添加分区，用于加载数据
		Write_Log_File "alter table ${TODIR} add partition(${partition_dir}) >> ${Log_File}"

    beeline -u "jdbc:hive2://hua-dlzx2-a0202:10000/default" -n ocdp -p 1q2w1q@W -e "alter table ${TODIR} add partition(${partition_dir})"
        if [ $? -eq 0 ]
        then
            Write_Log_File "$SOURCEDIR表数据装载成功"

        else
            Write_Log_File "$SOURCEDIR表数据装载失败"
            exit 1
        fi
