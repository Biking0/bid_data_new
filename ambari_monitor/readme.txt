更新时间：2019/10/12 17:35:27

ambari_monitor监控项目
功能描述：
1.监控Hbase集群各类服务（YARN、Hbase、kafka、flume等），主机是否掉线，是否出现警告消息
2.监控solr最新30分钟数据量，数据量低于100条触发告警
3.监控间隔10分钟

脚本位置：
ssh ocdp@10.218.146.65
ocdp123
/home/ocdp/hyn

功能模块：
	服务监控
	警告消息监控
	服务管理（启动、重启、停止）
	短信发送
	磁盘监控

项目结构：
	run.py               启动
	service_monitor.py   监控各类服务、警告消息
	solr_monitor.py      solr数据量监控
	config.py            常量配置文件
	send_sms.sh          短信通知通用脚本
	api_help             Ambari api接口帮助文档
	log:                 存放日志
		