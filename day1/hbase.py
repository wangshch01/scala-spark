# -*- encoding=utf-8 -*-

import os

# 导入thrift的python模块
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

# 导入自已编译生成的hbase python模块

from hbase import THBaseService
from hbase.ttypes import *

# 创建Socket连接，到s201:9090
transport = TSocket.TSocket('s201', 9090)
transport = TTransport.TBufferedTransport(transport)
protocol = TBinaryProtocol.TBinaryProtocol(transport)
client = THBaseService.Client(protocol)

transport.open()

# table = b'ns1:t1'
# row = b'row1'
# v1 = TColumnValue(b'f1', b'id', b'101')
# v2 = TColumnValue(b'f1', b'name', b'tomas')
# v3 = TColumnValue(b'f1', b'age', b'12')
# vals = [v1, v2, v3]
# put = TPut(row, vals)
# client.put(table, put)
# print("okkkk!!")
print("=======================")
#查询
# table = b'ns1:t1'
# rowkey = b'row1'
# col_id = TColumn(b"f1",b"id")
# col_name = TColumn(b"f1",b"name")
# col_age = TColumn(b"f1",b"age")
#
# cols = [col_id,col_name,col_age]
# get = TGet(rowkey,cols)
# res = client.get(table,get)
# print(bytes.decode(res.columnValues[0].qualifier))
# print(bytes.decode(res.columnValues[0].family))
# print(res.columnValues[0].timestamp)
# print(bytes.decode(res.columnValues[0].value))

# delete
table = b'ns1:t1'
rowkey = b'row1'
col_id = TColumn(b'f1',b'id')
col_name = TColumn(b'f1',b'name')
col_age = TColumn(b'f1',b'age')
#删除f1列族中的ID和name
cols = [col_id,col_name]

#构造删除对象
delete = TDelete(rowkey,cols)
res = client.deleteSingle(table,delete)
transport.close()
print("ok")