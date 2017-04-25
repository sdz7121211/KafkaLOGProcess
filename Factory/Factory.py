# -*- coding: utf-8 -*-
from abc import ABCMeta
from abc import abstractmethod


class Factory(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        pass

    @abstractmethod
    def create(self, *args, **kwargs):
        pass

