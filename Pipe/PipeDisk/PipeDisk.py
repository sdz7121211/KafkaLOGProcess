# -*- coding: utf-8 -*-
from collections import OrderedDict
import sys
import os
import gzip
from Pipe.Pipe import Pipe


class PipeDisk(Pipe):
    '''
    磁盘读/写控制类
    '''

    def __init__(self, cache_number = sys.maxint):
        # 缓存文件句柄
        self.sender_cache = OrderedDict()
        # self.sender_cache.popitem(last=False)

    def send(self, line, path, mode = "w", iscompress = True):
        '''
        :param line: 需要写入的数据
        :param path: 写入文件路径，不存在会自动创建，
        :param mode: 默认写入模式为 w，支持 w/a(追加) 两种模式
        :param iscompress:  是否压缩为gz格式文件
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
        if targetFileExtension == "gz":
            out = self.sender_cache[fullpath] if self.sender_cache.get(fullpath, None) else gzip.open(fullpath, mode)
        elif iscompress:
            fullpath = fullpath + ".gz"
            out = self.sender_cache[fullpath] if self.sender_cache.get(fullpath, None) else gzip.open(fullpath, mode)
        else:
            out = self.sender_cache[fullpath] if self.sender_cache.get(fullpath, None) else open(fullpath, mode)
        self.sender_cache.setdefault(fullpath, out)
        out.write(line.strip() + os.linesep)

    def recv(self, *args, **kwargs):
        '''
        :param args: 可选参数
        :param kwargs: 包含必选参数 path，所读取日志的路径
        :return:
        '''
        path = kwargs["path"]
        if os.path.exists(path):
            if not path.endswith(".gz"):
                _open = open
            else:
                _open = gzip.open
            file_handler = _open(path)
            for line in file_handler:
                yield line.strip()
            file_handler.close()
        else:
            print("Path is not exists: %s" % path)
            yield []

    def finish(self):
        '''
        使用send方法写文件时，完成时需要主动调用finish，保证数据写入磁盘
        :return:
        '''
        for file in self.sender_cache.keys():
            try:
                self.sender_cache.pop(file).close()
            except:
                import traceback
                print(traceback.print_exc())


    def close(self):
        self.finish()

    def __del__(self):
        '''
        当对象销毁时自动把数据写到磁盘上，不保证写入时机，尽量主动调用finish方法
        :return:
        '''
        self.finish()
        # for file in self.sender_cache.keys():
        #     try:
        #         self.sender_cache.pop(file).close()
        #     except:
        #         import traceback
        #         print(traceback.print_exc())


if __name__ == "__main__":
    pass