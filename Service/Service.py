# -*- coding: utf-8 -*-
import __init__
from abc import ABCMeta
from abc import abstractmethod
import threading
import time
from Configure.GetConfigure import GetConfigure


class Service(object):

    __metaclass__ = ABCMeta

    def __init__(self):
        self.configures = GetConfigure()
        self.load_config()

    @abstractmethod
    def load_config(self):
        pass

    @abstractmethod
    def start(self):
        _thread = threading.Thread(target=self.freshconfig)
        _thread.setDaemon(True)
        _thread.setName("Thread-Config")
        _thread.start()

    def freshconfig(self, once_sleep = 10):
        '''
        更新配置信息
        :param once_sleep:
        :return:
        '''
        while True:
            self.configures = GetConfigure()
            self.load_config()
            time.sleep(once_sleep)

