# encoding=utf8
import sys

reload(sys)
sys.setdefaultencoding('utf8')
import sqlparse

sql = """CREATE TABLE `alpha_sales_staff_info`(  `staff` string COMMENT '客服pin',  `mall_id` string COMMENT '服务商ID',  `brand_name` string COMMENT '品牌名称',  `category` string COMMENT '品类名称',  `level` string COMMENT '客服评级',  `group` string COMMENT 'AB test组别')PARTITIONED BY (  `dt` string)ROW FORMAT DELIMITED  FIELDS TERMINATED BY ' '  LINES TERMINATED BY '\n'STORED AS INPUTFORMAT  'org.apache.hadoop.mapred.TextInputFormat'OUTPUTFORMAT  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'"""
# 1.分割SQL
stmts = sqlparse.split(sql)
for stmt in stmts:

    # 2.format格式化
    print sqlparse.format(stmt, reindent=True, keyword_case="upper", encoding="utf-8")
    # 3.解析SQL
    stmt_parsed = sqlparse.parse(stmt)
    # print stmt_parsed[0].tokens[0]

    for i in range(len(stmt_parsed[0].tokens)):
        if stmt_parsed[0].tokens[i] != ' ':
            print stmt_parsed[0].tokens[i]

    # print stmt_parsed[0].tokens[1]
    # print stmt_parsed[0].get_name()
    # print stmt_parsed[0].get_alias()
    # print stmt_parsed[0].get_parent_name()
    # print stmt_parsed[0].get_real_name()
    # print stmt_parsed[0].get_sublists()
    # print stmt_parsed[0].get_type()
    # print stmt_parsed[0].get_type()
