# encoding=utf-8
# hive_table_check
# by hyn
# 20200224

# hadoop path
bdi_hadoop_path='hadoop fs -du -s -h  /user/hive/warehouse/asiainfoh.db/'

# table_name_list
# 表名，允许延迟时长

# 小时表，延迟单位，小时
hour_table=[
['dw_locl_wh_lacci_4g_time_yyyymmddhh',2]
]

# 日表，延迟单位，小时
day_table=[
['table_name',1]
]
