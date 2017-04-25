# -*- coding: utf-8 -*-
import time
import datetime
from MysqlClient import MysqlClient
from Configure.GetConfigure import GetConfigure


def producter_record(infos):
    '''
    解析数据记录入库
    :param infos:
    :return:
    '''
    config = GetConfigure()
    product_id = config.get("kafka", "product_id")
    client = MysqlClient("saas_server")
    con, cur = client.connection
    sql_format = "INSERT INTO saas_server.product_record (product_id, topic, tm, offset) VALUES (%s, %s, %s, %s)"
    for key in infos:
        topic = key
        values = infos[key]
        # tm = datetime.datetime.fromtimestamp(values["timestamp"]).strftime("%Y-%m-%d %H:%M:%S")
        tm = datetime.datetime.fromtimestamp(values["timestamp"])
        offset = values["offset"]
        cur.execute(sql_format, (product_id, topic, tm, offset))
    con.commit()
    client.closeMysql()


def productor_miss_record(num=None, timestamp_begin=None, timestamp_end=None, tm_begin=None, tm_end=None, last=None):
    '''
    获取解析失败的的数据，下一步用于自动缺失检测重传
    :param num:
    :param timestamp_begin:
    :param timestamp_end:
    :param tm_begin:
    :param tm_end:
    :param last:
    :return:
    '''
    config = GetConfigure()
    product_id = config.get("kafka", "product_id")
    if not (num is None):
        _tm_begin = time.strftime("%Y-%m-%d", time.localtime(time.time() - 86400 * num)) + " 00:00:00"
        _tm_end = time.strftime("%Y-%m-%d", time.localtime(time.time() - 86400 * num)) + " 23:59:00"
        _tm_now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()-60*10))
        _tm_end = _tm_now if _tm_end > _tm_now else _tm_end
    elif not (timestamp_begin is None):
        _tm_begin = time.strftime("%Y-%m-%d", time.localtime(timestamp_begin))
        if not (timestamp_end is None):
            _tm_end = time.strftime("%Y-%m-%d", time.localtime(timestamp_end))
        elif not (last is None):
            _tm_end = time.strftime("%Y-%m-%d", time.localtime(timestamp_begin+last*60))
        else:
            raise Exception("kwargs error!")
    elif not (tm_begin is None):
        _tm_begin = tm_begin
        if not (tm_end is None):
            _tm_end = tm_end
        elif not (last is None):
            _tm_end = (datetime.datetime.strptime(tm_begin, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(minutes=last)).strftime("%Y-%m-%d %H:%M:%S")
        else:
            raise Exception("kwargs error!")
    sql_format = "SELECT tm FROM saas_server.product_record WHERE (tm BETWEEN '%(_tm_begin)s' AND '%(_tm_end)s') AND product_id = '%(product_id)s' GROUP BY tm ORDER BY tm DESC"

    sql = sql_format % {"_tm_begin": _tm_begin, "_tm_end": _tm_end, "product_id": product_id}
    client = MysqlClient("saas_server")
    con, cur = client.connection
    cur.execute(sql)
    tms = set()
    for item in cur.fetchall():
        tms.add(item[0].strftime("%Y-%m-%d %H:%M") + ":00")
    tmp = list(tms)
    tmp.sort()
    print("mysql", tmp)
    return get_tms(_tm_begin, _tm_end) - tms


def get_tms(tm_begin, tm_end):
    # print(tm_begin, tm_end)
    begin = datetime.datetime.strptime(tm_begin, "%Y-%m-%d %H:%M:%S")
    end = datetime.datetime.strptime(tm_end, "%Y-%m-%d %H:%M:%S")
    index = begin
    tms = set()
    while index < end:
        tms.add(index.strftime("%Y-%m-%d %H:%M") + ":00")
        index += datetime.timedelta(minutes=1)
    # tmp = list(tms)
    # tmp.sort()
    # print("calc", tmp)
    return tms

if __name__ == "__main__":
    print productor_miss_record(tm_begin="2016-12-21 16:20:00", last=30)
    # print get_tms("2016-12-21 12:12:12", "2016-12-21 12:13:12")
