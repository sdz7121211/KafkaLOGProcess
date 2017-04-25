# -*- coding: utf-8 -*-
from Transform.Transform import Transform
import json
import time


class SaaSNewToSaaSOld(Transform):

    def __init__(self):
        self.basic_keys = [
            "jhd_ip",
            "jhd_ua",
            "jhd_os",
            "jhd_pb",
            "jhd_vr",
            "jhd_userkey",
            "jhd_datatype",
            "jhd_pushid",
            "jhd_auth",
            "jhd_sdk_version",
            "jhd_sdk_type",
            "sourcelog"
        ]

    def handle(self, line, sourcetype, desttype):
        if sourcetype == "SaaSLog" and desttype == "OldSaaSLog":
            return self.transform(line)
        elif sourcetype == "OldSaaSLog" and desttype == "SaaSLog":
            return self.retransform(line)

    def deformation(self, data):
        if "jhd_session" in data:
            return self.retransform(data)
        else:
            return self.transform(data)

    def transform(self, line, sourcelog = "SaaSLog"):
        if not isinstance(line, dict):
            data = json.loads(line)
        else:
            data = line
        # 生成日志标识，给没有 sourcelog 字段的日志，添加标识，仅转化指定的 sourcelog 日志
        data["sourcelog"] = data["sourcelog"] if "sourcelog" in data else sourcelog
        if data["sourcelog"] in [sourcelog]:
            result = {}
            # 初始化 jhd_session 字段
            result.setdefault("jhd_session", {})
            # 旧 ios sdk 日志格式， jhd_eventId 等于 jhd_opType
            jhd_eventId = data["jhd_eventId"]
            jhd_opType = data["jhd_opType"]
            data["jhd_eventId"] = jhd_opType
            jhd_opTime = data["jhd_opTime"].replace("-", "").replace(":", "").replace("+", "")
            # 旧 ios sdk 日志中包含, 添加 jhd_pageName 字段
            data["jhd_pageName"] = jhd_eventId if jhd_opType == "page" else None
            # 旧 ios sdk 日志中 日期格式为： yyyy-mm-dd+hh24:mi:ss
            ts = time.mktime(time.strptime(jhd_opTime, "%Y%m%d%H%M%S"))
            data["jhd_opTime"] = time.strftime(
                "%Y-%m-%d+%H:%M:%S",
                time.localtime(ts)
            )
            data["jhd_ts"] = int(ts) * 1000
            # 提取基础字段，不存在为 None
            map(lambda key: result.setdefault(key, data.pop(key) if key in data else None), self.basic_keys)
            # 非基础字段均放入 jhd_session 中
            map(lambda key: result["jhd_session"].setdefault(key, data[key]), [key for key in data.keys()])
            yield result
        else:
            yield

    def retransform(self, line, sourcelog = "OldSaaSLog"):
        if not isinstance(line, dict):
            data = json.loads(line)
        else:
            data = line
        # 生成日志标识，给没有 sourcelog 字段的日志，添加标识，仅转化指定的 sourcelog 日志
        data["sourcelog"] = data["sourcelog"] if "sourcelog" in data else sourcelog
        if data["sourcelog"] in [sourcelog]:
            session_data = data.pop("jhd_session") if data["jhd_session"] else {}
            # 把 jhd_session 中的字段提出来即可
            _result = dict({}, **data)
            _result = dict(_result, **session_data)
            # 去除旧日志中的 jhd_pageName
            jhd_opType = _result["jhd_opType"]
            if jhd_opType == "page":
                try:
                    _result["jhd_eventId"] = _result["jhd_pageName"]
                except:
                    _result["jhd_eventId"] = None
            else:
                _result["jhd_eventId"] = _result["jhd_eventId"] if "jhd_eventId" in _result else None
            if "jhd_pageName" in _result:
                del _result["jhd_pageName"]
            _result["jhd_opTime"] = _result["jhd_opTime"].replace("-", "").replace(":", "").replace("+", "")
            _result["jhd_ts"] = int(time.mktime(time.strptime(_result["jhd_opTime"], "%Y%m%d%H%M%S")))
            return _result
        else:
            return None


if __name__ == "__main__":
    line = '''{"jhd_pb": "AppStore", "jhd_vr": "2.2.1", "jhd_userkey": "014c2630-07c0-48f3-9f37-f24a973feb5c", "jhd_os": "iphone_9.2.1", "jhd_session": {"jhd_pageName": "null", "jhd_netType": "wifi", "jhd_map": {}, "jhd_interval": "null", "jhd_opTime": "2016-10-22+19:05:43", "jhd_opType": "action", "jhd_eventId": "ac44"}, "jhd_datatype": "feeling", "jhd_ua": "iPhone6_2", "jhd_ip": "223.79.146.228", "jhd_pushid": "57176ee739b0570057cda45f"}'''
    tester = SaaSNewToSaaSOld()
    result = tester.retransform(line)
    print("aaa", result)
    print("bbb", tester.deformation(line))


