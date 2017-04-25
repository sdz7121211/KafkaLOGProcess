# -*- coding: utf-8 -*-
import ConfigParser
from collections import OrderedDict


class ConfigParserCaseSensitive(ConfigParser.ConfigParser):

    def __init__(self, defaults=None):
        ConfigParser.ConfigParser.__init__(self, defaults=None)

    def optionxform(self, optionstr):
        return optionstr


class RawConfigParserCaseSensitive(ConfigParser.RawConfigParser):

    def __init__(self, defaults=None):
        ConfigParser.RawConfigParser.__init__(self, defaults=None, dict_type = OrderedDict)

    def optionxform(self, optionstr):
        return optionstr


if __name__ == "__main__":
    pass