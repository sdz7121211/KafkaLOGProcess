# -*- coding: utf-8 -*-
import time
import datetime
from MysqlClient import MysqlClient
from Common.timelimited import timelimited
from Configure.GetConfigure import GetConfigure


def consumer_offset(topic, time_str):
    '''
    获取指定topic指定时间的offset数据，下一步用于指定时间回补数据
    :param time_str: format is %Y-%m-%d %H:%M:%S
    :return:
    '''
    if type(time_str) == type(1) or type(time_str) == type(1.0):
        timestamp_sec = time_str - 60
    else:
        timestamp_sec = time.mktime(time.strptime(time_str, '%Y-%m-%d %H:%M:%S')) - 60
    time_begin = time.strftime("%Y-%m-%d %H:%M", time.localtime(timestamp_sec)) + ":00"
    time_end = time.strftime("%Y-%m-%d %H:%M", time.localtime(timestamp_sec+60)) + ":00"
    client = MysqlClient("saas_server")
    con, cur = client.connection
    sql_format = "SELECT topic, tm, MAX(offset) FROM saas_server.product_record WHERE topic = '%(topic)s' AND (tm >= '%(time_begin)s' AND tm < '%(time_end)s') GROUP BY topic, tm"
    sql = sql_format % {"topic": topic, "time_begin": time_begin, "time_end": time_end}
    cur.execute(sql)
    for item in cur.fetchall():
        topic, tm, offset = item[0], item[1], item[2]
    client.closeMysql()
    return offset + 1


@timelimited(50)
def consumer_record(infos):
    '''
    消费数据记录入库
    :param infos:
    :return:
    '''
    config = GetConfigure()
    group_id = config.get("kafka", "group_id")
    client = MysqlClient("saas_server")
    con, cur = client.connection
    # sql_format = "REPLACE INTO saas_server.custom_record(group_id, topic, tm, record_num, update_tm, update_num) VALUES (%s, %s, %s, %s, %s, %s)"
    sql_format = "INSERT INTO saas_server.custom_record(group_id, topic, tm, record_num, update_tm) VALUES (%s, %s, %s, %s, %s)"
    for topic in infos:
        for timestamp in infos[topic]:
            tm = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))
            update_tm = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
            record_num = infos[topic][timestamp]["record_num"]
            cur.execute(sql_format, (group_id, topic, tm, record_num, update_tm))
    con.commit()
    client.closeMysql()



if __name__ == "__main__":
    # print consumer_offset("topictest", "2016-12-21 15:15:03")
    # print consumer_offset("topictest", 1482304503.0)
    consumer_record({"test": {1472020442.988: 666}})