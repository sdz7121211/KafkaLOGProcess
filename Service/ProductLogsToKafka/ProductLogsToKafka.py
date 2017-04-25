# -*- coding: utf-8 -*-
import __init__
import threading
import sys
import time
import importlib
from ProductorService import ProductorService
from Common.JHDecorator import fn_timer
from Pipe.PipeDisk.PipeDisk import PipeDisk
from Pipe.PipeKafka.PipeKafka import PipeKafka
from Pipe.PipeKafka.PipelineKafka import PipelineKafka
from DBClient.ProducterRecord import producter_record
import logging
import json

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='/data1/pylogs/transform.log',
                    filemode='a')


class ProductLogsToKafka(ProductorService):

    def __init__(self):
        super(ProductLogsToKafka, self).__init__()
        super(ProductLogsToKafka, self).start()

    @fn_timer
    # def start(self, timestamp, tasks = ["ios", "android"], appointtopic = None, appkeyname = "jhd_datatype"):
    def start(self, timestamp, tasks = {"ios": {"appointtopic": None, "appkeyname": "jhd_datatype"}, "android": {"appointtopic": None, "appkeyname": "jhd_datatype"}}):
        '''
        :param timestamp: 日志生成时间
        :param tasks: 对应不同的日志类型
        :return:
        '''
        threads = {}
        for taskname in tasks:
            try:
                if taskname not in self.productor_config:
                    logging.warn("%s is not a valid task. Plaese check config.ini." % (taskname, ))
                    continue
                appointtopic, appkeyname = tasks[taskname]["appointtopic"], tasks[taskname]["appkeyname"]
                nginx_log_path = self.productor_config[taskname]["nginx_log_path"] # nigix 日志路径
                # 动态引入transform工场
                package_name = self.productor_config[taskname]["nigix_log_transformer_factory"].split(",")[0]
                module_name = self.productor_config[taskname]["nigix_log_transformer_factory"].split(",")[1]
                class_name = self.productor_config[taskname]["nigix_log_transformer_factory"].split(",")[2]
                load_module = importlib.import_module(package_name, module_name)
                class_factory = getattr(load_module, class_name)
                transformer = class_factory().create() # 创建transform
                nigix_log_path_backups = self.productor_config[taskname]["nigix_log_path_backups"] # 归档文件存放路径
                nigix_log_path_err = self.productor_config[taskname]["nigix_log_path_err"] # 解析出错日志输出路径
                task = threading.Thread(
                    name="Thread_%sProductor" % taskname,
                    target=self.productor,
                    args=(timestamp, nginx_log_path, nigix_log_path_backups, nigix_log_path_err, transformer, appointtopic, appkeyname)
                )
                threads.setdefault(taskname, task)
                threads[taskname].start()
            except:
                import traceback
                exc_type, exc_value, exc_traceback = sys.exc_info()
                errinfo = traceback.format_exception(exc_type, exc_value, exc_traceback)
                errinfo.append(taskname)
                logging.error(json.dumps(errinfo))


    def productor(self, timestamp, nginx_log_path, nigix_log_path_backups, nigix_log_path_err, transformer, appointtopic, appkeyname, *args):
        '''
        :param timestamp: 日志生成时间
        :param pathname: 对应原始日志路径在配置文件中的名称
        :param err_pathname: 解析出错是日志输出路径
        :return:
        '''
        cur_day = time.strftime("%Y%m%d", time.localtime(time.time()))
        yyyymmddhhmm = time.strftime("%Y%m%d%H%M", time.localtime(timestamp))
        yyyymmdd = yyyymmddhhmm[:8]
        hhmm = yyyymmddhhmm[8:]
        if yyyymmdd == cur_day:
            pathname = nginx_log_path
        else:
            pathname = nigix_log_path_backups
        path = pathname % {"yyyymmddhhmm": yyyymmddhhmm} # nigix日志路径

        pipline_kafka = PipelineKafka()  # 创建 pipeline
        recv_disk = PipeDisk()  # 创建接收
        send_kafka = PipeKafka()  # 创建发送
        pipline_kafka.add_recv(recv_disk, path=path)  # pipeline 添加 接收
        pipline_kafka.add_send(send_kafka, server_address=self.server_address)  # pipeline 添加 发送
        pipline_kafka.add_transformer(transformer)  # 添加解析
        # 测试模式
        if self.debug:
            errlog_path = self.debug_nigix_log_path_err % {"yyyymmdd": yyyymmdd, "hhmm": hhmm} # 测试错误log文件路径
            pipline_kafka.pipeline(
                appointtopic=self.debug_topic,
                appkeyname=appkeyname,
                errlog_path=errlog_path,
                timestamp_ms=int(timestamp) * 1000)  # 启动 pipeline
        # 补数模式
        elif appointtopic != None:
            errlog_path = nigix_log_path_err % {"yyyymmdd": yyyymmdd, "hhmm": hhmm}
            pipline_kafka.pipeline(
                appointtopic=appointtopic,
                appkeyname=appkeyname,
                errlog_path=errlog_path,
                timestamp_ms=int(timestamp) * 1000)  # 启动 pipeline
        # 正常模式指定appkeyname作为topic
        else:
            errlog_path = nigix_log_path_err % {"yyyymmdd": yyyymmdd, "hhmm": hhmm}
            pipline_kafka.pipeline(
                appkeyname=appkeyname,
                errlog_path=errlog_path,
                timestamp_ms=int(timestamp) * 1000)  # 启动 pipeline
        pipline_kafka.close()
        # 生产者数据入库
        infos = send_kafka.getSendTopicFutureInfo(timestamp = timestamp)
        try:
            producter_record(infos)
        except:
            logging.error(sys.exc_info(), json.dumps(infos))
        logging.info("Productor(%d,%s,%s) Success" % (int(timestamp), time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp)), path))

if __name__ == "__main__":

    if "run" in sys.argv:
        delay_minute = 5
        timestamp = time.time() - 60*5
        productor = ProductLogsToKafka()
        productor.start(timestamp)

    if "repair_aotu" in sys.argv:
        from DBClient.ProducterRecord import productor_miss_record
        tms = productor_miss_record(tm_begin="2016-10-31 00:00:00", tm_end="2016-10-31 00:00:00")
        for tm in tms:
            timestamp = time.mktime(time.strptime(tm, "%Y-%m-%d+%H:%M:%S"))
            productor = ProductLogsToKafka()
            productor.start(timestamp)
            time.sleep(1)

    if "repair" in sys.argv:
        start_tm = time.mktime(time.strptime('2016-10-31+00:00:00', '%Y-%m-%d+%H:%M:%S'))
        end_tm = time.mktime(time.strptime('2016-10-31+23:59:00', '%Y-%m-%d+%H:%M:%S'))
        while start_tm <= end_tm:
            yyyymmddhhmm = time.strftime("%Y%m%d%H%M", time.localtime(start_tm))
            yyyymmddhh = time.strftime("%Y%m%d%H", time.localtime(start_tm))
            productor = ProductLogsToKafka()
            productor.start(timestamp, tasks={"ios": {"appointtopic": "my-group_ios", "appkeyname": "jhd_datatype"}})
            time.sleep(0.5)
            start_tm += 60