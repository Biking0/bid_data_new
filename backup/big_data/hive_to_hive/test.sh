# 建表测试
create table test_hyn (id string ,name string);

# 插入数据
insert into  test_hyn (id,name) values ('1','123');


beeline -u "jdbc:hive2://192.168.190.88:10000/csap"
test_hyn
insert into  test_hyn (id,name) values ('1','123');


