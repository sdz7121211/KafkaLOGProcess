# -*- coding: utf-8 -*-'
import json
import logging
# from pykafka import KafkaClient
from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from Pipe.Pipe import Pipe
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

logger = logging.getLogger(__file__)

class PipeKafka(Pipe):

    def __init__(self):
        self.server_address = "10.25.115.53:9092,10.252.0.76:9092,10.44.184.245:9092"
        # self.client = KafkaClient(hosts="10.25.115.53:9092,10.252.0.76:9092,10.44.184.245:9092")
        self.producer = None
        self.consumer = None
        self.group_id = "my-group"
        self.topics_future = {}
        self.topics = {}
        self.products = {}

    def getSendTopicFutureInfo(self, **kwargs):
        result = {}
        for topic in self.topics_future:
            future = self.topics_future[topic]
            try:
                record_metadata = future.get(timeout=10)
            except KafkaError:
                logger.error(sys.exc_info())
                continue
            value = {
                "offset": record_metadata.offset,
                "partition": record_metadata.partition,
                "topic": record_metadata.topic
            }
            value.update(**kwargs)
            result[topic] = value
        return result

    def send(self, line, topic_name, timestamp_ms, **kwargs):
        '''
        :param line: 日志数据
        :param topic_name: kafka对应的topic，这里和appkey一致
        :param timestamp_ms: 设置kafka中消息时间戳
        :return:
        '''
        if self.producer is None:
            self.producer = KafkaProducer(
                bootstrap_servers=kwargs["server_address"] if "server_address" in kwargs else self.server_address,
                # value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                compression_type='gzip',
                retries = 3,
                api_version = (0, 10), # 支持自定义timestamp
            )

        topic_name = str(topic_name)
        # topic = self.client.topics[topic_name] if topic_name not in self.topics else self.topics[topic_name]
        # self.topics[topic_name] = topic
        # product = topic.get_producer(delivery_reports=False) if topic_name not in self.products else self.products[topic_name]
        # self.products[topic_name] = product
        # product.produce(line)
        future = self.producer.send(topic_name, value=line, timestamp_ms=timestamp_ms)
        self.topics_future[topic_name] = future

    def recv(self, topic_name, **kwargs):
        if self.consumer is None:
            self.consumer = KafkaConsumer(
                topic_name,
                group_id=kwargs["group_id"] if "group_id" in kwargs else self.group_id,
                bootstrap_servers=kwargs["server_address"] if "server_address" in kwargs else self.server_address,
                enable_auto_commit = False)
        for message in self.consumer:
            # logging.info("PipeKafka Message: %s" % str(message))
            yield message

    def commit_async(self, offsets=None, callback=None):
        if self.consumer:
            self.consumer.commit_async(offsets=None, callback=None)

    def commit(self, offsets=None):
        if self.consumer:
            self.consumer.commit(offsets=offsets)

    def close(self):
        try:
            if self.producer:
                self.producer.flush(timeout=600)
        except:
            import traceback
            print(traceback.print_exc())
        try:
            if self.producer:
                self.producer.close(timeout=600)
        except:
            import traceback
            print(traceback.print_exc())
        try:
            if self.consumer:
                self.consumer.close()
        except:
            import traceback
            print(traceback.print_exc())
        for product_topic in self.products.keys():
            try:
                self.products.pop(product_topic).stop()
            except:
                import traceback
                print(traceback.print_exc())


    # def __del__(self):
    #     try:
    #         if self.producer:
    #             # print(, self.producer)
    #             self.producer.flush(timeout=600)
    #     except:
    #         import traceback
    #         print(traceback.print_exc())
    #     try:
    #         if self.producer:
    #             self.producer.close(timeout=600)
    #     except:
    #         import traceback
    #         print(traceback.print_exc())
    #     try:
    #         if self.consumer:
    #             self.consumer.close()
    #     except:
    #         import traceback
    #         print(traceback.print_exc())
    #     for product_topic in self.products.keys():
    #         try:
    #             self.products.pop(product_topic).stop()
    #         except:
    #             import traceback
    #             print(traceback.print_exc())
