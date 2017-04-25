# -*- coding: utf-8 -*-
from Factory import Factory
from Transform.TransformSaaS.TransformSaaS import TransformSaaS


class TransformSaaSFactory(Factory):
    '''
    saas原始日志 --->>> 解析日志
    '''

    def __init__(self):
        pass

    def create(self, *args, **kwargs):
        return TransformSaaS()