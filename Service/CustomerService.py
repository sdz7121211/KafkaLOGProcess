# -*- coding: utf-8 -*-
import __init__
from abc import abstractmethod
from Service import Service


class CustomerService(Service):

    def __init__(self):
        super(CustomerService, self).__init__()

    def load_config(self):
        self.debug = self.configures.getboolean("debugconf", "debug")
        self.debug_topic = self.configures.get("debugconf", "debug_topic")

        self.group_id = self.configures.get("kafka", "group_id")
        self.server_address = self.configures.get("kafka", "hostname")

        # self.debug_collector_path = self.configures.get("debugconf", "debug_collector_path")

    @abstractmethod
    def start(self):
        super(CustomerService, self).start()



