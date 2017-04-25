# -*- coding: utf-8 -*-
import json
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import logging

from Pipe.Pipeline import Pipeline


class PipelineDisk(Pipeline):
    '''
    磁盘数据管道控制类
    '''

    def pipeline(self, *args, **kwargs):

        for data in [line for recv in self.recvs for line in recv()]:
            if not data:
                continue
            for transformer in self.transformers[:1]:
                try:
                    for data in transformer(data):
                        datatype = data[kwargs["appkeyname"]]
                        yyyymmdd = kwargs["yyyymmdd"]
                        hhmm = kwargs["hhmm"]
                        path_format = kwargs.get("path_format", "/data1/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log")
                        disk_path = path_format % {"datatype": datatype, "yyyymmdd": yyyymmdd, "hhmm": hhmm}
                        for send in self.sends:
                            try:
                                if data.get("isdevuser", False) == False:
                                    send(json.dumps(data, ensure_ascii=False).encode('utf-8') if isinstance(data, dict) else data, path = disk_path)
                                else:
                                    send(json.dumps(data, ensure_ascii=False).encode('utf-8') if isinstance(data, dict) else data, path = kwargs["devuser_path"])
                            except:
                                if data.get("isdevuser", False) == False:
                                    send(json.dumps(data) if isinstance(data, dict) else data, path = disk_path)
                                else:
                                    send(json.dumps(data, ensure_ascii=False).encode('utf-8') if isinstance(data, dict) else data, path = kwargs["devuser_path"])
                except:
                    import traceback
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    errinfo = traceback.format_exception(exc_type, exc_value, exc_traceback)
                    errinfo.append(data)
                    logging.warning(json.dumps(errinfo))