import os

hdfs1_sh = "hdfs haadmin -getServiceState nn1"
hdfs2_sh = "hdfs haadmin -getServiceState nn2"
hdfs3_sh = "hdfs haadmin -getServiceState nn3"

result1 = os.popen(hdfs1_sh).readline()
result2 = os.popen(hdfs2_sh).readline()
result3 = os.popen(hdfs3_sh).readline()

print type(result1), result1
print type(result2), result2
print type(result3), result3
