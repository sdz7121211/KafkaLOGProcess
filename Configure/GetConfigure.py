# -*- coding: utf-8 -*-
from ConfigParserCaseSensitive import RawConfigParserCaseSensitive
from os import path

base_path = path.dirname(path.abspath(__file__))


class GetConfigure(object):

    # func demo
    # secs = cf.sections()
    # opts = cf.options("sec_a")
    # kvs = cf.items("sec_a")
    # str_val = cf.get("sec_a", "a_key1")
    # int_val = cf.getint("sec_a", "a_key2")
    # cf.set("sec_b", "b_key3", "new-$r")
    # cf.add_section('a_new_section')

    def __init__(self):
        self.cf = RawConfigParserCaseSensitive()
        self.cf.read(path.join(base_path, "conf.ini"))

    def get_productor_config(self):
        productor_path = {}
        nigix_log_paths = self.cf.options("nigix_log_path")
        nigix_log_transformer_factorys = self.cf.options("nigix_log_transformer_factory")
        nigix_log_path_backups = self.cf.options("nigix_log_path_backups")
        nigix_log_path_errs = self.cf.options("nigix_log_path_err")
        for nigix_log_path, nigix_log_transformer_factory, nigix_log_path_backups, nigix_log_path_err in \
                zip(nigix_log_paths, nigix_log_transformer_factorys, nigix_log_path_backups, nigix_log_path_errs):
            productor_path.setdefault(nigix_log_path, {
                "nginx_log_path": self.cf.get("nigix_log_path", nigix_log_path),
                "nigix_log_transformer_factory": self.cf.get("nigix_log_transformer_factory", nigix_log_transformer_factory.strip()),
                "nigix_log_path_backups": self.cf.get("nigix_log_path_backups", nigix_log_path_backups),
                "nigix_log_path_err": self.cf.get("nigix_log_path_err", nigix_log_path_err)
                })
        return productor_path

    def get_consumer_config(self):
        consumer_path = {}


    def get(self, section, key):
        return self.cf.get(section, key)

    def getboolean(self, section, key, default = None):
        if section in self.cf.sections() and key in self.cf.options(section):
            return self.cf.getboolean(section, key)
        else:
            return default


if __name__ == "__main__":
    tester = GetConfigure()
    # print tester.getboolean("basicsss", "debug"), type(tester.getboolean("basic", "debug"))
    # print tester.get("paths", "jhsaaslogs_err")
    print tester.get_productor_paths("ios")