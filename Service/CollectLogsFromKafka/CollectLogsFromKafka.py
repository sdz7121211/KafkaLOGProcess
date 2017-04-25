# -*- coding: utf-8 -*-
import __init__
import sys
import time
import json
import threading
import logging
from CustomerService import CustomerService
from DBClient.MysqlClient import MysqlClient
from Pipe.PipeKafka.PipeKafka import PipeKafka
from Pipe.PipeDisk.PipeDiskKafkaConsumer import PipeDiskKafkaConsumer
from Pipe.PipeDisk.PipelineKafkaToDisk import PipelineKafkaToDisk
from DBClient.ConsumerRecord import consumer_record

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')


class CollectLogsFromKafka(CustomerService):
    '''
    从kafka消费数据
    '''
    def __init__(self):
        super(CollectLogsFromKafka, self).__init__()
        self.thread_collectors = {}

    def start(self):
        super(CollectLogsFromKafka, self).start()
        # 创建任务：获取group_id对应的appkey
        _thread = threading.Thread(name="Thread-KafkaTopics", target=self.getTopics)
        _thread.setDaemon(True)
        _thread.start()

        print("Thread-KafkaTopics start...")
        time.sleep(5)
        # 创建消费者任务：需要参数，topic（appkey）、输出路径格式
        _thread_collector = threading.Thread(name="ThreadCollector-Father", target=self.collectorControl)
        _thread_collector.start()

    def collectorControl(self, once_sleep = 60):
        while True:
            for topic, logpath in self.topics:
                # 创建消费线程
                if topic not in self.thread_collectors.keys():
                    try:
                        _thread = threading.Thread(name="ThreadCollector-%s" % topic, target=self.kafkaConsumer, args=(topic, logpath))
                        _thread.start()
                    except:
                        import traceback
                        print(traceback.print_exc())
                        logging.error(sys.exc_info())
                    self.thread_collectors.setdefault(topic, _thread)
                else:
                    # 检测消费进程是否存活，如果异常退出就删除该任务,
                    collector = self.thread_collectors[topic]
                    if not collector.isAlive():
                        self.thread_collectors.pop(topic)
            time.sleep(once_sleep)

    def kafkaConsumer(self, topic, logpath):
        pipeline = PipelineKafkaToDisk()
        recv = PipeKafka()
        send = PipeDiskKafkaConsumer()
        send_callback = recv.commit_async
        pipeline.add_recv(recv, topic, group_id = self.group_id, server_address = self.server_address)
        pipeline.add_send(send, send_callback = send_callback)
        try:
            t = threading.Thread(target=self.record, args=(pipeline.records,))
            t.setDaemon(True)
            t.start()
        except:
            log.error(sys.exc_info())
        pipeline.pipeline(path_format=self.configures.get("collector_log_path", logpath))
        pipeline.close()

    def record(self, records, once_sleep=60):
        while True:
            infos = {}
            cur_timestamp = time.time()
            for key in records:
                topic = key
                timestamps = records[key].keys()
                timestamps.sort()
                for timestamp in timestamps:
                    if cur_timestamp - timestamp > 60:
                        record = records[key].pop(timestamp)
                        infos.setdefault(topic, {}).setdefault(timestamp, {}).update(**record)
            if infos:
                consumer_record(infos)
            log.info(json.dumps(infos))
            time.sleep(once_sleep)

    def getTopics(self, once_sleep = 60):
        '''
        获取需要消费的topic，可通过数据库表控制
        :param once_sleep: topics更新频率
        :return:
        '''
        while True:
            if self.debug:
                debug_topic = self.configures.get("debugconf", "debug_topic")
                self.topics = [(item, "debug") for item in debug_topic.split(",")]
            else:
                kafka_topics = set()
                saas_appkey = set()
                appkey_logpath = {}
                try:
                    from kafka import SimpleClient
                    hostname = self.configures.get("kafka", "hostname")
                    client = SimpleClient(hosts=hostname)
                    for topic in client.topics:
                        kafka_topics.add(topic)
                    client.close()
                    log.info("get kafka topics: %s" % json.dumps(list(kafka_topics)))
                except:
                    logging.error(sys.exc_info())
                    continue

                try:
                    client = MysqlClient("saas_server")
                    topics = client.getTopics(group_id = self.group_id)
                    for topic, logpath in topics:
                        saas_appkey.add(topic)
                        appkey_logpath.setdefault(topic, set()).add(logpath)
                    client.closeMysql()
                    log.info("get mysql appkeys: %s" % json.dumps(list(saas_appkey)))
                except:
                    logging.error(sys.exc_info())
                    continue
                self.topics = [(topic, logpath) for topic in list(kafka_topics & saas_appkey) for logpath in appkey_logpath[topic]]
            log.info("current topics: %s" % json.dumps(self.topics))
            time.sleep(once_sleep)

if __name__ == "__main__":
    if "run" in sys.argv:
        collector = CollectLogsFromKafka()
        collector.start()

    if "repair" in sys.argv:
        start_tm = time.mktime(time.strptime('2016-10-31+00:00:00', '%Y-%m-%d+%H:%M:%S'))
        end_tm = time.mktime(time.strptime('2016-10-31+23:59:00', '%Y-%m-%d+%H:%M:%S'))
        yyyymmddhhmm = time.strftime("%Y%m%d%H%M", time.localtime(start_tm))
        yyyymmddhh = time.strftime("%Y%m%d%H", time.localtime(start_tm))
        collector = CollectLogsFromKafka()
        collector.start()
