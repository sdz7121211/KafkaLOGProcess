# -*- coding: utf-8 -*-
from MysqlClient import MysqlClient
import datetime
import time

client = MysqlClient("saas_server")
con, cur = client.connection

sql_format = "INSERT INTO saas_server.product_record (topic, tm, offset) VALUES (%s, %s, %s)"
topic = "test"
tm = datetime.datetime.fromtimestamp(time.time())
offset = 666

cur.execute(sql_format, (topic, tm, offset))

con.commit()
client.closeMysql()

