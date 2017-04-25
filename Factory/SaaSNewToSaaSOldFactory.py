# -*- coding: utf-8 -*-
from Factory.Factory import Factory
from Transform.SaaSNewToSaaSOld.SaaSNewToSaaSOld import SaaSNewToSaaSOld


class SaaSNewToSaaSOldFactory(Factory):
    '''
    基础信息从map中提取出来：saas新格式日志 ---->>>>> 基础信息在map字段中：saas旧格式日志
    '''

    def __init__(self):
        pass

    def create(self, *args, **kwargs):
        return SaaSNewToSaaSOld()