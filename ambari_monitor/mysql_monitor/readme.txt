20210129

dataos元数据库连接数监控

show status like 'Threads_connected%';


mysql -h ritdsdataos.mysql.svc.huaarmcore.hpc -P20001 -udataos_dev   -pqXliH9*Ro#qDGomY dataos_dev

mysql -hritdsdataos.mysql.svc.huaarmcore.hpc -P20001 -udataos_pro -Ddataos_pro -pJklCTLKsF7KiW9YF

mysql -h ritdsdataos.mysql.svc.huaarmcore.hpc -P20001 -udataos_dev   -pqXliH9*Ro#qDGomY dataos_dev -e "show status like 'Threads_connected%'; ""