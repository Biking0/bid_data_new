20201013

config.py
sy_data_check.py
ocdp_data_check.py
conn_db.py

新老库数据量稽核程序方法：

1.新建配置表：表名，周期，稽核标识字段；
2.新建日志表：表名，周期，数据量，新老库标识字段（old，new），更新时间；
3.配置表获取表名，作为任务，更新稽核标识字段；
4.新老库两个程序，按表名进行group by，获取周期及对应数据量；
5.日志表数据插入数据：插入数据之前，根据【表名】、【周期】、【新老库标识字段】作为唯一性检查该数据是否存在，若存在，删除该条数据；若不存在则插入新数据；
6.新老库两个程序，更新及插入数据时，更新新老库标识字段。

