# -*- encoding=utf-8-*-
import pymysql
try:
    #连接mysql数据库
    con = pymysql.connect(host='localhost',user='root',passwd='root',db='python',port=3306,charset='utf8')
    #conn = pymysql.connect(host='localhost', user='root', passwd='root', db='test', port=3306, charset='utf8')
    #打开游标
    cur = con.cursor()
    #执行sql
    i = 1
    while i < 100:
        sql = "insert into t1(name,age) VALUES('%s',%d)" % ("tom" + str(i),i);
        print(sql)
        cur.execute(sql)
        i += 1
    con.commit()
    cur.close()
    con.close()

except  Exception:
    print("yichang")

