脚本在65服务器上，65服务器已配置其他服务器的互信

登录97服务器
ssh ocdp@10.93.171.97
ocdp123

脚本路径
cd /home/ocdp/hyn

执行脚本清理kafka各服务器日志
sh remote_run_py.sh

#######################
脚本介绍

python清理日志脚本，现在保留两天的日志，可以修改参数
clean_kafka_log.py

remote_run_py.sh
将python脚本拷贝到各远程服务器，并执行python脚本
