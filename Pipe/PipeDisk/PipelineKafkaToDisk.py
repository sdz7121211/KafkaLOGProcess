# -*- coding: utf-8 -*-
import time
import json
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import logging

from Pipe.Pipeline import Pipeline


class PipelineKafkaToDisk(Pipeline):
    '''
    kafka -> 磁盘 数据流控制类
    '''

    def pipeline(self, *args, **kwargs):
        # for data in [line for recv in self.recvs for line in recv()]:
        for data in self.recvs[0]():
            if not data:
                continue
            try:
                datatype = None
                topic = data.topic
                value = data.value
                try:
                    value_loads = json.loads(value)
                    datatype = value_loads["jhd_datatype"]
                except:
                    logging.warning(sys.exc_info())
                datatype = topic if datatype is None else datatype
                timestamp_sec = data.timestamp/1000.0
                yyyymmdd = time.strftime("%Y%m%d", time.localtime(timestamp_sec))
                hhmm = time.strftime("%H%M", time.localtime(timestamp_sec))
                # path_format = kwargs.get("path_format", "/data1/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log")
                path_format = kwargs["path_format"]
                disk_path = path_format % {"datatype": datatype, "yyyymmdd": yyyymmdd, "hhmm": hhmm}

                for send in self.sends:
                    try:
                        send(json.dumps(value, ensure_ascii=False).encode('utf-8') if isinstance(value, dict) else value, disk_path)
                    except:
                        send(json.dumps(value) if isinstance(value, dict) else value, disk_path)
                # self.records[datatype] = {timestamp_sec, {}}
                self.records.setdefault(datatype, {}).setdefault(timestamp_sec, {})
                self.records[datatype][timestamp_sec]["record_num"] = self.records[datatype][timestamp_sec].get("record_num", 0) + 1
            except:
                import traceback
                exc_type, exc_value, exc_traceback = sys.exc_info()
                errinfo = traceback.format_exception(exc_type, exc_value, exc_traceback)
                errinfo.append(data)
                logging.warning(json.dumps(errinfo))