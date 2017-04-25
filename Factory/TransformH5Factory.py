# -*- coding: utf-8 -*-
from Factory.Factory import Factory
from Transform.TransformH5.TransformH5 import TransformH5


class TransformH5Factory(Factory):
    '''
    H5日志解析：原始日志 ---- >>>> 解析日志
    '''

    def __init__(self):
        pass

    def create(self, *args, **kwargs):
        return TransformH5()