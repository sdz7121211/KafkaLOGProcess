# -*- coding: utf-8 -*-
import json
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import time
import logging

from Pipe.Pipeline import Pipeline
from Pipe.PipeDisk import PipeDisk


class PipelineKafka(Pipeline):
    '''
    磁盘 -> kafka 数据列控制类
    '''

    def pipeline(self, *args, **kwargs):
        '''
        :param args:
        :param kwargs:
                参数： 1、appkeyname：标记appkey的字段名称，
                       2、timestamp_ms：日志接收时间，精确到秒
        :return:
        '''
        log_count = 0 # 记录日志条数
        for data in [line for recv in self.recvs for line in recv()]:
            if not data:
                continue
            for transformer in self.transformers[:1]:
                try:
                    for data in transformer(data):
                        # 没有指定topic，则根据appkeyname对应key作为topic
                        if "appointtopic" not in kwargs:
                            topic = data[kwargs.get("appkeyname", "jhd_datatype")]
                        else:
                            topic = kwargs["appointtopic"]
                        timestamp_ms = kwargs.get("timestamp_ms", time.time() * 1000)
                        for send in self.sends:
                            try:
                                if data.get("isdevuser", False) == False:
                                    send(json.dumps(data, ensure_ascii=False).encode('utf-8') if isinstance(data, dict) else data, topic, timestamp_ms)
                                else:
                                    send(json.dumps(data, ensure_ascii=False).encode('utf-8') if isinstance(data, dict) else data, "devuser", timestamp_ms)
                            except:
                                if data.get("isdevuser", False) == False:
                                    send(json.dumps(data) if isinstance(data, dict) else data, topic, timestamp_ms)
                                else:
                                    send(json.dumps(data) if isinstance(data, dict) else data, "devuser", timestamp_ms)
                        log_count += 1
                except:
                    import traceback
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    errinfo = traceback.format_exception(exc_type, exc_value, exc_traceback)
                    errinfo.append(data)
                    errlog = PipeDisk.PipeDisk()
                    errlog.send(json.dumps(errinfo), kwargs["errlog_path"])
        logging.info("LogCount: %d" % log_count)