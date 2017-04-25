# -*- coding: utf-8 -*-
from Factory.Factory import Factory
from Transform.TransformH5.TransformH5ToSaaS import TransformH5toSaaS
from Transform.TransformH5.TransformH5 import TransformH5


class TransformH5toSaaSFactory(Factory):
    '''
    H5解析日志 --->>> App格式日志
    '''

    def __init__(self):
        pass

    def create(self, *args, **kwargs):
        # return lambda *_args, **_kwargs: TransformH5toSaaS.transform(lambda :TransformH5())
        return TransformH5toSaaS()