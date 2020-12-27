bdi@HACC-BH02-SERVICE118:/dev/shm> cat /home/bdi/Asiainfo/tas/loc/zhiji/sms/dw_tra_imsi_stay_yyyymm.py
##!/usr/bin/env python
## -*-coding:utf-8 -*-
##******************************************************************************
## **  文件名称： dw_tra_region_phone_info_min.py
## **  功能描述：
## **             1:语音呼叫话单
## **             2:短信话单
## **             3:位置更新话单
## **             4:寻呼话单
## **             5:开关机话单
## **             6:位置更新话单
## **  输 入 表： tb_dwd_mid_ci_loc_merge_hour              23g:1-6
## **  输 出 表： tb_dwd_ci_loc_merge_hour
## **  创 建 者： chengpx
## **  创建日期： 2017年11月10日
## **  修改日志：
## **  修改日期：
## *******************************************************************************
## **  程序调用格式：python tb_dwd_ci_loc_merge_mc_hour.py 2015073001
## *******************************************************************************

#导入baseModule模块，然后定义子集的class，必须继承BaseObject，
#然后定义一个函数，把业务逻辑写入函数内，在函数头上加上装饰器，则自动实现通用逻辑的调用
#业务逻辑如果太长则可以定义多个函数，然后在主函数（就是加装饰器的函数）内按子集的业务逻辑依次调用
from baseModule_min import  *

class VoiceCall(BaseObject):
    @deco
    def run(self):
        mydict = self.constants
        #=====================================

        #整理活动id和区域id和基站
        hivesql = []
        hivesql.append('''
        create table if not exists %(target_tab)s
        (
           imsi           string
          ,prov_id        string
          ,city_id        string
          ,county_id      string
          ,region_id      string
          ,flag_prov      string
          ,flag_city      string
          ,flag_cou       string
          ,flag_reg       string
          ,flag_loc       string
          ,city_home      string
          ,user_status_id string
          ,serv_number  string
          ,yl_01          string
          ,yl_02          string
        )
        partitioned by (MONTH_ID string)
        row format delimited
        fields terminated by '|'
        stored as rcfile
        ''' % mydict)
        HiveExe(hivesql,self.name,self.dates)

        #==用户常驻信息(省市)
        hivesql = []
        hivesql.append('''drop table if exists %(tmp_01)s ''' % mydict)
        hivesql.append('''
        create table %(tmp_01)s
        as
        select
               case when a.imsi is not null then a.imsi else b.imsi end as imsi
              ,a.prov_id
              ,a.day_cnt day_cnt_prov
              ,a.flag    flag_prov
              ,b.city_id
              ,b.day_cnt day_cnt_city
              ,b.flag    flag_city
          from
               (
               select
                  imsi
                 ,'371'  prov_id
                 ,day_cnt
                 ,flag
               from %(stay_prov)s
               where month_id='%(ARG_OPTIME_MONTH)s'
                     and flag > 0
               ) a
          full outer join
               (
               select
                 imsi
                ,city_id
                ,day_cnt
                ,flag
               from %(stay_city)s
               where month_id='%(ARG_OPTIME_MONTH)s'
                     and flag > 0
               ) b
               on a.imsi = b.imsi
        ''' % mydict)
        HiveExe(hivesql,self.name,self.dates)

        #==用户常驻信息(省市县)
        hivesql = []
        hivesql.append('''drop table if exists %(tmp_02)s ''' % mydict)
        hivesql.append('''
        create table %(tmp_02)s
        as
        select
               case when a.imsi is not null then a.imsi else b.imsi end as imsi
              ,a.prov_id
              ,a.day_cnt_prov
              ,a.flag_prov
              ,a.city_id
              ,a.day_cnt_city
              ,a.flag_city
              ,b.county_id
              ,b.day_cnt day_cnt_cou
              ,b.flag    flag_cou
          from
               %(tmp_01)s a
          full outer join
               (
               select
                 imsi
                ,county_id
                ,day_cnt
                ,flag
               from %(stay_cou)s
               where month_id='%(ARG_OPTIME_MONTH)s'
                     and flag > 0
               ) b
               on a.imsi = b.imsi
        ''' % mydict)
        HiveExe(hivesql,self.name,self.dates)

        #==用户常驻信息(省市县逻辑区)
        hivesql = []
        hivesql.append('''drop table if exists %(tmp_03)s ''' % mydict)
        hivesql.append('''
        create table %(tmp_03)s
        as
        select
               case when a.imsi is not null then a.imsi else b.imsi end as imsi
              ,a.prov_id
              ,a.day_cnt_prov
              ,a.flag_prov
              ,a.city_id
              ,a.day_cnt_city
              ,a.flag_city
              ,a.county_id
              ,a.day_cnt_cou
              ,a.flag_cou
              ,b.region_id
              ,b.day_cnt day_cnt_reg
              ,b.flag    flag_reg
          from
               %(tmp_02)s a
          full outer join
               (
               select
                 imsi
                ,region_id
                ,day_cnt
                ,flag
               from %(stay_region)s
               where month_id='%(ARG_OPTIME_MONTH)s'
                     and flag > 0
               ) b
               on a.imsi = b.imsi
        ''' % mydict)
        HiveExe(hivesql,self.name,self.dates)

        #==用户常驻信息(省市县逻辑区白名单)
        hivesql = []
        hivesql.append('''drop table if exists %(tmp_04)s ''' % mydict)
        hivesql.append('''
        create table %(tmp_04)s
        as
        select
               case when a.imsi is not null then a.imsi else b.imsi end as imsi
              ,a.prov_id
              ,a.day_cnt_prov
              ,a.flag_prov
              ,a.city_id
              ,a.day_cnt_city
              ,a.flag_city
              ,a.county_id
              ,a.day_cnt_cou
              ,a.flag_cou
              ,a.region_id
              ,a.day_cnt_reg
              ,a.flag_reg
              ,b.write_id
          from
               %(tmp_03)s a
          full outer join
               (
               select
                 imsi
                ,'1'    write_id
               from %(stay_white)s
               where
                     -- month_id='%(ARG_OPTIME_MONTH)s'
                     month_id='201903'
               ) b
               on a.imsi = b.imsi
        ''' % mydict)
        #HiveExe(hivesql,self.name,self.dates)

        #==结果表
        hivesql = []
        hivesql.append('''
        insert overwrite table %(target_tab)s partition (month_id='%(ARG_OPTIME_MONTH)s')
        select
           a.imsi
          ,a.prov_id
          ,a.city_id
          ,a.county_id
          ,a.region_id
          ,a.flag_prov
          ,a.flag_city
          ,a.flag_cou
          ,a.flag_reg
          ,case when b.imsi is not null then 1 else 0 end as flag_loc
          ,b.area_code                                       city_home
          ,b.user_status_id                                  user_status_id 
          ,b.serv_number
          ,null
          ,null
        from %(tmp_03)s a
        left outer join 
           (
            select
                imsi
               ,area_code
               ,user_status_id
               ,serv_number
            from 
               dwd_dw_sc_user_imsi_yyyymmdd
            where 
                  day_id='%(ARG_OPTIME_LASTMONEND)s'
           ) b
        on a.imsi = b.imsi
        ''' % mydict)
        HiveExe(hivesql,self.name,self.dates)

        #删除临时表
        hivesql = []
        hivesql.append('''drop table if exists %(tmp_01)s ''' % mydict)
        hivesql.append('''drop table if exists %(tmp_02)s ''' % mydict)
        hivesql.append('''drop table if exists %(tmp_03)s ''' % mydict)
        hivesql.append('''drop table if exists %(tmp_04)s ''' % mydict)
        HiveExe(hivesql,self.name,self.dates)

    def __init__(self):
        BaseObject.__init__(self)
        mydict = self.constants
        mydict['dba']                    = "asiainfoh."
        mydict['source_dim_segment']     = mydict['dba']+"dim_loc_number_segment"                          #维表：号段 ods_in_rs_boss_number_segment_x
        mydict['source_dim_country']     = mydict['dba']+"dim_loc_country_id"                              #维表：国家
        mydict['source_dim_country']     = mydict['dba']+"dim_loc_country_id"                              #维表：省市
        mydict['source_dim_ci']          = mydict['dba']+"dim_loc_lac_ci"                                  #维表：基站维表
        #--------------------------------------------------------------------------------------------------
        mydict['source_tab']             = "dwd_loc_mc_lte_merge_min"                                      #信令融合表，分钟单位
        #mydict['dim_region']            = "dim_tra_region_lac_ci"
        mydict['dim_region']             = "dim_region_3_laccell"           #
        mydict['dim_region_sms']         = "td_sms_rule"                    #短信规则表
        mydict['dim_user']               = "dw_target_customer_info"        #短信规则和手机号码
        mydict['stay_prov']              = "dw_tra_prov_stay_bh_yyyymm"
        mydict['stay_city']              = "dw_tra_city_stay_bh_yyyymm"
        mydict['stay_cou']               = "dw_tra_county_stay_bh_yyyymm"
        mydict['stay_region']            = "dw_tra_region_stay_bh_yyyymm"
        mydict['stay_white']             = "dwd_tra_white_stay_yyyymm"
        mydict['target_tab']             = "dw_tra_imsi_stay_yyyymm"         #目标表_知迹用户常驻信息
        #--------------------------------------------------------------------------------------------------
        mydict['dim_xy_sms']             = "dim_sms_xy_loc_ci"              #信阳南湾湖水库基站维度明细

        mydict['tmp_01']                 = 'tmp_'+mydict['target_tab']+mydict['ARG_OPTIME_MONTH']+"_01"
        mydict['tmp_02']                 = 'tmp_'+mydict['target_tab']+mydict['ARG_OPTIME_MONTH']+"_02"
        mydict['tmp_03']                 = 'tmp_'+mydict['target_tab']+mydict['ARG_OPTIME_MONTH']+"_03"
        mydict['tmp_04']                 = 'tmp_'+mydict['target_tab']+mydict['ARG_OPTIME_MONTH']+"_04"

if __name__ == '__main__':
    voicecall = VoiceCall()
    voicecall.run()


