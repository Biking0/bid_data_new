#!/bin/bash
# ***************************************************************************
# 文件名称：code_head.sh
# 功能描述：shell代码头
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20191216
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：sh code_head.sh
# ***************************************************************************

# 华为服务器认证
source /opt/fi_client/bigdata_env
kinit -kt /home/bdi/user.keytab asiainfouser1

echo 'code_head_test'

#!/bin/bash
set -x
source /opt/fi_client/bigdata_env
kinit -kt /home/bdi/user.keytab asiainfouser1