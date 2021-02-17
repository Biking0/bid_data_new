20200828
vertica库数据稽核

表数量1000张左右
表分散在不同的库里

原语句
select a.table_schema,a.table_name,a.column_name,a.data_type from columns a inner join (
 select table_schema,table_name,max(column_id) mx_column_id from columns where data_type like 'varchar%'
 group by 1,2) b on a.column_id = b.mx_column_id


select a.table_schema,a.table_name,a.column_name,a.data_type from columns a inner join (
 select table_schema,table_name,max(column_id) mx_column_id from columns where data_type =int
 group by 1,2) b on a.column_id = b.mx_column_id

select a.table_schema,a.table_name,a.column_name,a.data_type from columns a inner join (
 select table_schema,table_name,max(column_id) mx_column_id from columns where data_type like 'varchar%'
 group by 1,2) b on a.column_id = b.mx_column_id  and a.table_name=''


取所有int
select  column_name from columns where  data_type ='int' and table_name=''
select  column_name from columns where  and table_name=''

拼接求和
select to_char(sum(num1))||'_'||to_char(sum(num2)) from table_name ;

取int求和
select sum() from  where table_name=''


取最后一个字符串
select a.column_name from columns a inner join (
 select table_schema,table_name,max(column_id) mx_column_id from columns where data_type like 'varchar%'
 group by 1,2) b on a.column_id = b.mx_column_id and a.table_name=''

最后一个字符串求和
sum(length)

20200830
vt

1.获取表名，多线程处理
2.获取最后一个字符串，获取int
3.判断是否为分区表，是分区表需要指定日期，添加where条件
4.插入稽核结果表

多int字段测试,tb_dwd_ct_85ct_call_list_hour

20200903
v3
添加，最后一个字符串字段count
同时支持正式库、测试库

其最后一个字段
select a.table_schema,a.table_name,a.column_name,a.data_type from columns a inner join (
 select table_schema,table_name,max(ordinal_position) mx_ordinal_position from columns where data_type like 'varchar%'
 and table_name=''  group by 1,2) b on a.ordinal_position = b.mx_ordinal_position  and a.table_name=''

