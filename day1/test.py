#-*- encoding=utf-8-*-
import pymysql

try:
    conn = pymysql.connect(host='localhost', user='root', passwd='root', db='test', port=3306, charset='utf8')
    cur = conn.cursor()
    cur.execute('select version()')
    version = cur.fetchone()
    print(version)
    cur.close()
    conn.close()
except  Exception:
    print("发生异常")