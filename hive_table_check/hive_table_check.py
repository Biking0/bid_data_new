# encoding=utf-8
#********************************************************************************
# 文件名称：hive_table_check.py
# 功能描述：华为hive表延迟监控，监控分区
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200220
# 修改日志：20200420
# 修改日期：
# ****************************************************************************
# 程序调用格式：nohup python hive_table_check.py >> nohup.out &
# *******************************************************************************

# encoding=utf-8
# hive_table_check
# by hyn
# 20200224

import os
import time
import datetime
import config

input_data='202002'

day_hour=['01','02','03','04','05','06','07','08',
                    '09','10','11','12','13','14','15','16',
                    '17','18','19','20','21','22','23','24',]

def main():
    for i in config.hour_table:
        
        month_str=datetime.datetime.now().strftime('%Y%m')
        day_str=datetime.datetime.now().strftime('%Y%m%d')
        
        
        #hour_str=datetime.datetime.now().strftime('%Y%m%d%H')
        hour_str=(datetime.datetime.now()-datetime.timedelta(hours=i[1])).strftime('%Y%m%d%H')
        
        
        
        table_name = i[0]
        print table_name
        table_info_sh=config.bdi_hadoop_path+table_name+'/month_id='+month_str+'/day_id='+day_str+'/hour_id='+hour_str+'/ > table_info.txt'
        print table_info_sh
        
        os.popen(table_info_sh)
        
        
        f=open('table_info.txt').readlines()
        print f
        print type(f)
        # 分区不存在
        if len(f) == 0 :
			
			#告警时间
			now_time=' 告警时间：'+time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())
            print 'bdi平台任务有延迟：',i[2]
			
			sms_info='bdi平台任务有延迟：'+i[2])+now_time
            
            send_chk_sh='sh wh_create_chk.sh yiqing 20200109 '
            
            print send_chk_sh
            
            os.popen(send_chk_sh).readlines()
            continue
            
        print f[0].split(' ')
        result=f[0].split(' ')
		
		# 数据为空
        if not result[1]=='G':
            print 'bdi平台任务数据为空',i[2]
            
            send_chk_sh='sh wh_create_chk.sh yiqing 20200109 '
            
            print send_chk_sh
            
            os.popen(send_chk_sh).readlines()
            continue
        print '疫情小时任务运行正常'



#demo()
            
            
        
        
