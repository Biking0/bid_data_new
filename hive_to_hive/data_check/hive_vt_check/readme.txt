20210417


功能模块：
    hive同名表稽核
    hive稽核表稽核
    vt表稽核
    稽核结果汇总

项目结构：
    run.py                   启动
    main.py                  主入口
    config.py                通用配置文件
    mysql_conn_db.py         连接mysql
    vt_conn_db.py            连接vt
    hive_data_check.py       hive同名表稽核
    hive_chk_data_check.py   hive稽核表稽核
    vt_data_check.py         vt表稽核


hive下沉vertica稽核

20210425
目前只支持单周期日
