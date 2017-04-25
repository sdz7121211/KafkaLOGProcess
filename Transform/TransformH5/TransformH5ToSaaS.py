# -*- coding: utf-8 -*-
import json
import time
import re
from Transform.Transform import Transform


def find_value_fromdict_singlekey(dict_a, find_key):
    assert isinstance(dict_a, dict), "dict_a is not type(dict)"
    if find_key in dict_a:
        return dict_a[find_key]
    for x in range(len(dict_a)):
        temp_key = dict_a.keys()[x]
        temp_value = dict_a[temp_key]
        # if temp_key == find_key:
        #     return temp_value
        if isinstance(temp_value, dict):
            result = find_value_fromdict_singlekey(temp_value, find_key)
            if result != None:
                return result
        else:
            continue

class TransformH5toSaaS(Transform):

    def __init__(self):
        self.pattern_ua = re.compile(r''';\s*?(\S*?)\s*?Build/''', re.I)
        self.comparison_table = {
            "jhd_datatype": "appkey",
            "jhd_userkey": "uid",
            "jhd_ip": "ip",
            "jhd_opType": "type",
            "jhd_vr": "vr",
            "jhd_ts": "ts",
            "jhd_map": "usermap",
            "jhd_pageName": "uri",
            "jhd_eventId": "event",
            "jhd_interval": "value",
            "jhd_ua": "ua",

            "device": "device",
            "browser": "browser",
            "system": "system",
            "support": "support",
            "ref": "ref",
            "value": "value",
            "device": "device"
        }

        self.comparison_key = {
            "ac": "action",
            "dur": "jhf_dur",
            "page": "page"
        }

    def retransform(self, *args, **kwargs):
        pass

    def deformation(self, *args, **kwargs):
        pass

    def transform(self, line):
        data = line if isinstance(line, dict) else json.loads(line)
        # singlekey
        datatype = find_value_fromdict_singlekey(data, self.comparison_table["jhd_datatype"])
        userkey = find_value_fromdict_singlekey(data, self.comparison_table["jhd_userkey"])
        userkey = userkey.strip()

        # appendkey
        vr = find_value_fromdict_singlekey(data, self.comparison_table["jhd_vr"])
        ip = find_value_fromdict_singlekey(data, self.comparison_table["jhd_ip"])
        ip = ip.strip("/").strip('''"''')
        ua = find_value_fromdict_singlekey(data, self.comparison_table["jhd_ua"])
        try:
            device_name = find_value_fromdict_singlekey(data, self.comparison_table["device"]).get("name", None)
        except:
            device_name = None
        if device_name:
            tmp = device_name.lower()
            if "android" in tmp:
                m = self.pattern_ua.search(ua)
                if m:
                    ua = m.group(1)
                    ua = ua.lower()
            else:
                ua = tmp
        try:
            system_name = find_value_fromdict_singlekey(data, self.comparison_table["system"]).get("name", None)
        except:
            system_name = None
        useros = system_name
        if system_name:
            tmp = system_name.lower()
            if "android" in tmp:
                useros = "android"
            elif "iphone" in tmp:
                useros = "ios"
        # other
        ts = find_value_fromdict_singlekey(data, self.comparison_table["jhd_ts"])
        try:
            optime = time.strftime("%Y%m%d%H%M%S", time.localtime(int(ts/float(1000))))
        except:
            optime = time.strftime("%Y%m%d%H%M%S", time.time())
        usermap = find_value_fromdict_singlekey(data, self.comparison_table["jhd_map"])
        if usermap == "" or usermap == "null" or (not usermap):
            usermap = {}
        # countkey
        opa = find_value_fromdict_singlekey(data, self.comparison_table["jhd_opType"])
        opa = self.comparison_key.get(opa, opa)
        eventid = find_value_fromdict_singlekey(data, self.comparison_table["jhd_eventId"]) if opa == "action" else opa
        pagename = "jhd_page" if opa == "page" else None
        try:
            uri = find_value_fromdict_singlekey(data, self.comparison_table["jhd_pageName"])
            if opa == "action":
                value = find_value_fromdict_singlekey(data, self.comparison_table["value"])
                usermap["uri"] = uri
                usermap["value"] = value
            elif opa == "page":
                ref = find_value_fromdict_singlekey(data, self.comparison_table["ref"])
                usermap["uri"] = uri
                usermap["ref"] = ref
            elif opa == "end":
                value = find_value_fromdict_singlekey(data, self.comparison_table["value"])
                usermap["uri"] = uri
                usermap["value"] = value
        except:
            import traceback
            print(traceback.print_exc())
            yield
        yield {
                    "jhd_auth": "off",
                    "jhd_sdk_type": "js",
                    "jhd_ua": ua,
                    "jhd_map": usermap,
                    "jhd_loc": None,
                    "jhd_pushid": None,
                    "jhd_opTime": optime,
                    "jhd_eventId": eventid if eventid != "page" else pagename,
                    "jhd_ip": ip,
                    "jhd_pb": None,
                    "jhd_userkey": userkey,
                    "jhd_os": useros,
                    "jhd_opType": opa,
                    "jhd_sdk_version": vr,
                    "jhd_netType": None,
                    "jhd_vr": vr,
                    "jhd_ts": ts,
                    "jhd_interval": usermap["value"] if opa == "end" else None,
                    "jhd_datatype": datatype
                    }


if __name__ == "__main__":
    line = '''{"appkey": "BQ_H5", "uid": "222DF41A-004F-4365-A604-0E67B2CDC104", "usermap": {"tag": 0, "title": "\u53d1\u73b0"}, "ref": "http://discovery.biqu365.com/", "support": "21", "uri": "http://discovery.biqu365.com/", "ts": 1478621407881, "vr": "1.3.2", "ua": "Mozilla/5.0 (iPhone; CPU iPhone OS 9_3_4 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Mobile/13G35", "ip": "117.136.13.250", "device": {"version": "-1", "name": "iphone"}, "browser": {"version": "-1", "name": "webview"}, "type": "page", "screen": "320*568", "system": {"version": "9.3.4", "name": "iPhone OS"}}'''
    # line = r'''{"appkey": "BQ_H5", "status": "end", "uid": "1478679567577_eisdst2423", "ua": "Mozilla/5.0 (iPhone; CPU iPhone OS 9_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13B143 Safari/601.1", "screen": "414*736", "uri": "http://192.168.1.61:8950/embed/subs/alternates?webviewsource=1#datastatistic://jh_dataStatistics", "value": 1752, "ts": 1478679573205, "vr": "1.3.2", "ip": "\"125.33.203.240\"", "device": {"version": "-1", "name": "iphone"}, "browser": {"version": "9.0", "name": "safari"}, "type": "dur", "system": {"version": "9.1", "name": "iPhone OS"}}'''
    # tester = TransformH5toApp()
    # print tester.transform(line)


