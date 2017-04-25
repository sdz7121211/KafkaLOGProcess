# -*- coding: utf-8 -*-
from abc import ABCMeta
from abc import abstractmethod


class Pipe(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        pass

    @abstractmethod
    def send(self, *args, **kwargs):
        '''
        数据发送端
        '''
        pass

    @abstractmethod
    def recv(self, *args, **kwargs):
        '''
        数据接收端
        '''
        pass

    @abstractmethod
    def close(self):
        pass

