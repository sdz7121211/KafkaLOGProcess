# -*- coding: utf-8 -*-
import json
import sys
from abc import ABCMeta
from abc import abstractmethod
import logging
log = logging.getLogger(__name__)


class Pipeline(object):

    __metaclass__ = ABCMeta

    def __init__(self):
        self.recvs = [] # 保存接收端对象
        self.recvs_closer = []

        self.sends = [] # 保存发送端对象
        self.sends_closer = []

        self.transformers = [] # 保存日志解析对象

        self.records = {}

    def add_recv(self, reciver, *args, **kwargs):
        '''
        :param reciver: Pipe子类，接收数据
        :param args: 可选
        :param kwargs: 可选
        :return:
        '''
        self.recvs.append( lambda *_args, **_kwargs: reciver.recv(*(list(args) + list(_args)), **(dict(kwargs, **_kwargs)) ))
        self.recvs_closer.append( lambda *_args, **_kwargs: reciver.close( *_args, **_kwargs) )


    def add_send(self, sender, *args, **kwargs):
        '''
        :param sender: Pipe子类，数据发送端
        :param args: 可选
        :param kwargs: 可选
        :return:
        '''
        self.sends.append( lambda *_args, **_kwargs: sender.send( *(list(args) + list(_args)), **(dict(kwargs, **_kwargs)) ))
        self.sends_closer.append( lambda *_args, **_kwargs: sender.close(  *_args, **_kwargs) )

    def add_transformer(self, transformer, *args, **kwargs):
        '''
        :param transformer: Transform 子类，对日志进行解析
        :param args: 可选
        :param kwargs: 可选
        :return:
        '''
        self.transformers.append(lambda *_args, **_kwargs: transformer.transform( *(list(args) + list(_args)), **(dict(kwargs, **_kwargs)) ))

    @abstractmethod
    def pipeline(self, *args, **kwargs):
        '''
        数据管道控制方法，根据需要继承实现
        '''
        pass

    def close(self):
        for recv_close in self.recvs_closer:
            try:
                recv_close()
            except:
                import traceback
                item = sys.exc_info()
                exce_data = traceback.format_exception(item[0], item[1], item[2])
                # print(type(exce_data), exce_data)
                exce_data.append(str(type(recv_close)))
                exce_data.append(str(recv_close))
                log.warn(json.dumps(exce_data))

        for send_close in self.sends_closer:
            try:
                send_close()
            except:
                import traceback
                item = sys.exc_info()
                exce_data = traceback.format_exception(item[0], item[1], item[2])
                exce_data.append(str(type(recv_close)))
                exce_data.append(str(recv_close))
                log.warn(json.dumps(exce_data))
