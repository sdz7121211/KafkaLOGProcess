#coding=utf-8
from __init__ import configPath
import MySQLdb
import ConfigParser
import sys
reload(sys)
sys.setdefaultencoding("utf-8")


class MysqlClient(object):
    cf = ConfigParser.ConfigParser()
    cf.read(configPath)
    mysql_host = cf.get("mysqldb", "mysql_host")
    mysql_port = cf.getint("mysqldb", "mysql_port")
    mysql_user = cf.get("mysqldb", "mysql_user")
    mysql_passwd = cf.get("mysqldb", "mysql_passwd")

    def __init__(self, db, host=mysql_host, port=mysql_port, user=mysql_user, passwd=mysql_passwd):
        self.db = db
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.con, self.cur = self._connectMysql

    @property
    def _connectMysql(self):
        conn = MySQLdb.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                passwd=self.passwd,
                db=self.db,
                charset='utf8'
                )
        cur = conn.cursor()
        return conn, cur

    @property
    def connection(self):
        return self.con, self.cur

    def select(self, cmd):
        try:
            self.cur.execute(cmd)
            print("success: ", cmd)
        except:
            import traceback
            print(traceback.print_exc())
            print("faild: ", cmd)
        for item in self.cur.fetchall():
            yield item

    def getTopics(self, **kwargs):
        result = []
        columns_name = ["group_id", "client_id", "topic", "plat"]
        # sql = "SELECT group_id, client_id, appkey topic, plat FROM saas_meta.d_kafka_log_sumary WHERE enable = 1"
        sql = "SELECT group_id, client_id, appkey topic, plat, logpath FROM saas_server.d_log_collector WHERE enable = 1"
        self.cur.execute(sql)
        for item in self.cur.fetchall():
            column_filter = [True] * len(item)
            for index, column in enumerate(columns_name):
                if kwargs.get(column, None) != None:
                    if item[index] != kwargs[column]:
                        column_filter[index] = False
                        break
            if all(column_filter):
                result.append(item)
        return [(item[2], item[4]) for item in result]

    def closeMysql(self):
        self.cur.close()
        self.con.close()


if __name__ == "__main__":
    tester = MysqlClient("saas_meta")
    # print(tester.con, tester.cur)
    print(tester.getTopics(plat = "666"))