# -*- coding: utf-8 -*-
from collections import OrderedDict
import threading
import time
import os
import gzip
import sys
import logging
from PipeDisk import PipeDisk


class PipeDiskKafkaConsumer(PipeDisk):
    '''
    kafka数据写磁盘控制类
    '''

    def __init__(self):
        # super self.sender_cache format: {full_path: [fileobj, lastaccess_ts]}
        super(PipeDiskKafkaConsumer, self).__init__()
        # 后台线程检查长时间未使用的文件句柄，并关闭
        _thread = threading.Thread(name="Thread-fileCollection", target=self.fileCollection)
        _thread.setDaemon(True)
        _thread.start()

    def send(self, line, path, mode = "a", iscompress = True, send_callback = None, **kwargs):
        '''
        :param line: 需要写入的数据
        :param path: 写入文件路径，不存在会自动创建，
        :param mode: 默认写入模式为 w，支持 w/a(追加) 两种模式
        :param iscompress:  是否压缩为gz格式文件
        :param send_callback: 回调函数，用于执行offset提交
        :param kwargs:
        :return:
        '''
        os.sep = "/"
        fullpath = path.replace("\\", "/")
        path, file = fullpath.rsplit(os.sep, 1)[0], fullpath.rsplit(os.sep, 1)[1]
        targetFilename, targetFileExtension = file.rsplit(".", 1)[0], file.rsplit(".", 1)[1]
        if " " in path:
            return
        if not os.path.exists(path):
            os.makedirs(path)
        # if os.path.exists(fullpath) and (time.time() - os.path.getmtime(fullpath)) >= 600:
        #     os.remove(fullpath)
        #     logging.info("remove old file %s" % fullpath)
        if targetFileExtension == "gz":
            out = self.sender_cache[fullpath][0] if self.sender_cache.get(fullpath, None) else gzip.open(fullpath, mode)
        elif iscompress:
            fullpath = fullpath + ".gz"
            out = self.sender_cache[fullpath][0] if self.sender_cache.get(fullpath, None) else gzip.open(fullpath, mode)
        else:
            out = self.sender_cache[fullpath][0] if self.sender_cache.get(fullpath, None) else open(fullpath, mode)
        out.write(line.strip() + os.linesep)
        self.sender_cache.setdefault(fullpath, [out, time.time(), send_callback])
        self.sender_cache[fullpath][1] = time.time()
        logging.debug("%s@%s@%s" % (line, path, fullpath))

    def fileCollection(self, once_sleep = 5):
        '''
        检测长时间未使用的文件句柄，并关闭
        :param once_sleep:
        :return:
        '''
        while True:
            handle_record = set()
            for key in self.sender_cache.keys():
                try:
                    lastaccess_ts = self.sender_cache[key][1]
                    delta_ts = time.time() - lastaccess_ts
                    if delta_ts >= 10:
                        item = self.sender_cache.pop(key)
                        callback = item[2]
                        if callback and id(callback) not in handle_record:
                            callback()
                            handle_record.add(id(callback))
                        item[0].close()
                        logging.info("Closed %s File." % key)
                except:
                    logging.error(sys.exc_info())
            time.sleep(once_sleep)

    def close(self):
        for key in self.sender_cache.keys():
            try:
                item = self.sender_cache.pop(key)
                item[0].close()
            except:
                import traceback
                import json
                exc_type, exc_value, exc_traceback = sys.exc_info()
                errinfo = traceback.format_exception(exc_type, exc_value, exc_traceback)
                logging.warning(json.dumps(errinfo))


    # def __del__(self):
    #     for key in self.sender_cache.keys():
    #         try:
    #             self.sender_cache.pop(key)[0].close()
    #             logging.info("Closed %s File." % key)
    #         except:
    #             logging.warning(sys.exc_info())

