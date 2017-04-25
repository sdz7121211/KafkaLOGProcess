# -*- coding: utf-8 -*-
import __init__
from abc import abstractmethod
from Service import Service


class ProductorService(Service):

    def __init__(self):
        super(ProductorService, self).__init__()

    def load_config(self):
        # debug 配置
        self.debug = self.configures.getboolean("debugconf", "debug")
        self.debug_topic = self.configures.get("debugconf", "debug_topic")
        self.debug_nigix_log_path_err = self.configures.get("debugconf", "debug_nigix_log_path_err")
        # self.debug_collector_path = self.configures.get("debugconf", "debug_collector_path")
        # kafak hostnames
        self.server_address = self.configures.get("kafka", "hostname")
        # 生产日志路径
        self.productor_config = self.configures.get_productor_config()

    @abstractmethod
    def start(self):
        super(ProductorService, self).start()



