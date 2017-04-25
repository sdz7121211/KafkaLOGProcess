#-*- coding: utf-8 -*-
import re
import urllib
import json
from Transform.Transform import Transform


class TransformParmsToSaaS(Transform):

    def __init__(self):
        self.pattern_pairs = re.compile('(\w+)=([^&\s]+)')
        self.pattern_map = re.compile('(\w+)=([^$]*)')
        self.corname_dict = {}

    def retransform(self, *args, **kwargs):
        pass

    def deformation(self, *args, **kwargs):
        pass

    def transform(self, line):
        result = {}
        result["ip"] = line.split(",")[0]
        for item in self.pattern_pairs.findall(line):
            key = item[0] if not self.corname_dict.get(item[0], None) else self.corname_dict[item[0]]
            value = item[1] if key not in set(["session", "datatype"]) else urllib.unquote(item[1])
            if key not in set(['sig', 'sver']):
                result[key] = value
        # result["session"] = self.parse_session(result.get("session", ""))
        session = result["session"]
        for session_item in session.split("@"):
            result["session"] = self.parse_session(session_item)
            result_jh = self.jh_head(result)
            if result_jh["jhd_datatype"] == "feeling":
                result_jh["jhd_userkey"] = result_jh["jhd_userkey"].lower()
            if result_jh["jhd_datatype"] == "guagua" and result_jh["jhd_session"]["jhd_opType"] == "end" \
                    and "e" in result_jh.get("jhd_interval", ""):
                print line
                continue
            yield self.rule.applyRule(result_jh)
            # yield result_jh

    def jh_head(self, result):
        tmp = {}
        for key in result:
            tmp.setdefault("jhd_%s" % key, result[key]) if not key.startswith("jhd_") else tmp.setdefault(key, result[key])
        return tmp

    def parse_session(self, session):
        session_keys = ["jhd_eventId", "jhd_map", "jhd_opType", "jhd_opTime", "jhd_interval", "jhd_netType", "jhd_pageName"]
        session_dic = dict(zip(session_keys, ['null'] * len(session_keys)))
        items = session.split("#")
        opTime = items[0]
        netType = items[1]
        # 根据2016.08.03日约定仅有 in、end、page、action这4个动作，其余均归入action
        opType = items[2] if items[2] in set(["in", "end", "page", "action"]) else "action"
        eventId = session.split("type=")[1].split("$")[0] if items[2] == "action" else None
        pageName = session.split("id=")[1].split("$")[0] if items[2] == "page" else None
        interval = session.split("dur=")[1].split("$")[0] if "dur=" in session else None
        interval = interval if interval else "null"
        session_dic["jhd_opTime"] = opTime if opTime else "null"
        session_dic["jhd_netType"] = netType if netType else "null"
        session_dic["jhd_opType"] = opType if opType else "null"
        session_dic["jhd_eventId"] = eventId if eventId else items[2]
        session_dic["jhd_pageName"] = pageName if pageName else "null"
        session_dic["jhd_interval"] = interval if interval else "null"
        session_dic["jhd_map"] = self.item_pairs_map(session)
        if opType == 'action' and  "type" in session_dic["jhd_map"]:
            del session_dic["jhd_map"]["type"]
        elif opType == 'page' and  "id" in session_dic["jhd_map"]:
            del session_dic["jhd_map"]["id"]
        # elif interval and  "dur" in session_dic["jhd_map"]:
        #     del session_dic["jhd_map"]["dur"]
        return session_dic

    def isJson(self, str):
        try:
            json.loads(str)
            return True
        except:
            return False

    def item_pairs_map(self, session):
        result = {}
        for item in self.pattern_map.findall(session):
            key = item[0]
            value = item[1] if item[1] else "null"
            if key in set(["remark", "loc"]) and self.isJson(value):
                dic_info = json.loads(value)
                if dic_info:
                    [result.setdefault(_key, dic_info[_key]) for _key in dic_info]
            else:
                result[key] = value
        return result


if __name__ == "__main__":
    line = '''117.136.63.52,-,[01/Jun/2016:17:26:01 +0800],"GET /appsta.js?session=2016-06-01%2B17%3A26%3A01%234g%23action%23type%3Dac44%24remark%3D&datatype=feeling&os=iphone_8.3&vr=2.0.1&pb=AppStore&userkey=C72C4C25-836C-4657-B3D7-C9F330C523A2&ua=iPhone6_2&pushid=5745933375c4cd66580f239e&sver=1.2&sig=1105d3220b4988d1c05424f9f355d468 HTTP/1.1",204 0,"-","feeling/208 CFNetwork/711.3.18 Darwin/14.0.0" "-"'''
    line = '''101.201.150.227,-,[23/Sep/2016:16:52:35 +0800],"POST /appsta.js HTTP/1.1",/appsta.js?datatype=guaeng&os=iphone_9.30&vr=1.2.0816&pb=appstore&userkey=B5C15B20-887B-492A-871E-8CD0F4FC899F&ua=iphone_6s_plus_(a1634/a1687/a1690/a1699)&pushid=70f6ea966b07f90eda5efeff7867ce1c&isupdate=1&session=2016-09-23%2B16%3A52%3A33%23wifi%23v%23id%3D%E6%B5%8B%E8%AF%95%E9%A2%91%E9%81%93_%2ABiscuit%60s%2ABig%2AFriend_watch%24dur%3D28.0%24adur%3D44.0%24pnum%3D0%24ref%3Dhome&sver=V3.4&sig=97e8b2eb069e9cb176ae10a6a304f4dc,200 18,"-","curl/7.19.7 (x86_64-redhat-linux-gnu) libcurl/7.19.7 NSS/3.15.3 zlib/1.2.3 libidn/1.18 libssh2/1.4.2" "-"'''
    transform_tester = Transform()
    for item in transform_tester.transform(line):
        print item
