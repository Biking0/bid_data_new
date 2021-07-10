#!/bin/bash
# ***************************************************************************
# �ļ����ƣ�dw_label_quanyi_yyyymm_ftp110.sh
# ���������������ݵ��ӿڻ�110
# �� �� ��10.93.171.83��ocetl
# �� ��     dw_label_quanyi_yyyymm_ftp110
# �� �� ��
# �� �� �ߣ�hyn
# �������ڣ�20200228
# �޸���־��
# �޸����ڣ�
# ***************************************************************************
# ������ø�ʽ��sh test.sh test 20191231
# ������ø�ʽ��sh dw_label_quanyi_yyyymm_ftp110.sh dw_label_quanyi_yyyymm 202002
# ***************************************************************************

# ��̨����
#set -x

# �����������
table_name=$1
month_id=$(echo $2|cut -c1-6 )
#day_id=$2


# ���ݱ��ش��·������hdfs�������ݴ�ŵ�����
local_data_path="/hdfs/data9/port_files/"

data_path=${local_data_path}dw_label_quanyi_${month_id}

mkdir $data_path

#touch ${local_data_path}dw_label_quanyi1_${month_id}/dw_label_quanyi_${month_id}.txt

#echo ${table_name}_$month_id
#echo ${local_data_path}${table_name}_$month_id.txt

# �������ݴ���ļ�
echo ${local_data_path}dw_label_quanyi_${month_id}/dw_label_quanyi_${month_id}.txt

# hive�������ļ�
hive -e "set hive.exec.compress.output=false;set hive.cli.print.header=false;
select concat(
trim(statis_month),'|',
trim(user_id),'|',
city_id,'|',
trim(phone_no),'|',
trim(label_id)) from dw_label_quanyi_yyyymm where month_id=${month_id} ;" >${local_data_path}dw_label_quanyi_${month_id}/dw_label_quanyi_${month_id}.txt;

# sftpЭ�鴫�ļ���110

sftp ftpintf@10.93.171.110 <<EOF

mkdir /export/home/out_file/quanyi
cd /export/home/out_file/quanyi
lcd ${local_data_path}dw_label_quanyi_${month_id}

binary
mput *

bye
EOF

#binary
#mput *




