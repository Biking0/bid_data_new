20200712 1440
添加hive表最后一列校验

功能：
统计最后一列string类型字段，找到最后一列
统计hive表最后一个string类型字段的所有数据长度总和，数据内容先不管
1. 求和
2. 列数据抽取到文件进行md5对比
3. 记录没有统计end_string hive表名，导出到文件

select sum(length()) from table_name;