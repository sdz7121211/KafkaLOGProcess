# -*- coding: utf-8 -*-
from Factory.Factory import Factory
from Transform.TransformParmsToSaaS.TransformParmsToSaaS import TransformParmsToSaaS


class TransformParmsToSaaSFactory(Factory):
    '''
    通过URL参数上报日志 --->>> saas格式日志（json格式）
    '''

    def __init__(self):
        pass

    def create(self, *args, **kwargs):
        return TransformParmsToSaaS()