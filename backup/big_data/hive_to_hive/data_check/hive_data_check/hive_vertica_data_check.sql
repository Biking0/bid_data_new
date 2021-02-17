# 数据稽核结果表，建表语句
CREATE TABLE CHK_RESULT (
 DATA_SOURCE    STRING          COMMENT '数据源'
,DES_TBL        STRING          COMMENT '目标表名'
,CYCLICAL       STRING          COMMENT '周期'
,COUNT1         STRING          COMMENT '数据统计'
,SUM1           STRING          COMMENT '求和'
,REMARK         STRING          COMMENT '备注'
,CHK_DT         string             COMMENT '检核时间'
)
ROW FORMAT DELIMITED FILEDS TERMINATED BY ' ';

insert into table CHK_RESULT

# 稽核结果分区表
create table chk_result (
 data_source    string          comment '数据源'
,des_tbl        string          comment '目标表名'
,cyclical       string          comment '周期'
,count1         string          comment '数据统计'
,sum1           string          comment '求和'
,remark         string          comment '备注'
,chk_dt         string             comment '检核时间'
)
partitioned by (static_date string)
row format delimited fileds terminated by ' ';

create table chk_result_test12333 (
 data_source    string          comment '数据源'
,des_tbl        string          comment '目标表名'
,cyclical       string          comment '周期'
,count1         string          comment '数据统计'
,sum1           string          comment '求和'
,remark         string          comment '备注'
,chk_dt         string             comment '检核时间'
)
partitioned by (statis_date string)

# 字段注释
 DATA_SOURCE    STRING          COMMENT '数据源'
,DES_TBL        STRING          COMMENT '目标表名'
,CYCLICAL       STRING          COMMENT '周期'     按周期（分区）条件查询数据
,COUNT1         STRING          COMMENT '数据统计'
,SUM1           STRING          COMMENT '求和'     sum(colume1-colume2-colume3)
,REMARK         STRING          COMMENT '备注'
,CHK_DT         int             COMMENT '检核时间'

select 'DATA_SOURCE',table_name,'partition',count(*),concat(nvl(sum(id),''),nvl(sum(name),'')),'REMARK',from_unixtime(unix_timestamp()) from table_name where patitions='';

# 获取当前时间
select from_unixtime(unix_timestamp());

样例：
hive原表

test_hyn
id(int)

对应的hive稽核表
test_hyn_check

table_name string
count_data string
sum_column1 string
remark string
chk_dt string


# 非分区表
对应hive表

# 插入数据

# sql拼接字段
--select concat(nvl(leix01,''),nvl(leix02,''),nvl(leix03,'')) from dim_ivr_dictionary where ivr_table like 'zj%' and bm='40102'
create table test_hyn_concat (id int ,name int) ;
insert into test_hyn_concat (id,name ) values (2,3);
insert into test_hyn_concat (id,name ) values (4,5);
--select concat(nvl(id,''),nvl(name,'')) from test_hyn_concat;

# 按列求和再进行拼接
select concat(nvl(sum(id),''),nvl(sum(name),'')) from test_hyn_concat;







