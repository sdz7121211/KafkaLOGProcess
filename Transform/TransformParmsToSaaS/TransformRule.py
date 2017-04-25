import time


class TransformRule(object):

    def __init__(self):
        self.head = "jhd_"
        self.lower_keys = ["".join([self.head, item]) for item in ["netType", "ua", "os"]]
        self.rename_keys = dict( [("".join([self.head, item[0]]), "".join([self.head, item[1]])) \
                                  for item in [("location", "region")]] )
        self.combine_keys = [("".join([self.head, item[0]]), "".join([self.head, item[1]])) \
                             for item in [("pageName", "eventId")]]

    def applyRule(self, data):
        self.drawSession(data, "jhd_session")
        self.addHead(data)
        self.doLower(data)
        self.doRename(data)
        self.doCombine(data)
        self.addTs(data)
        return data

    def doLower(self, data):
        for key in self.lower_keys:
            if key not in data:
                continue
            data[key] = data[key].lower()

    def doRename(self, data):
        for key in self.rename_keys:
            data.setdefault(self.rename_keys[key], data[key]) if key in data else None
            if key in data:
                del data[key]

    def doCombine(self, data):
        for item in self.combine_keys:
            key_name = item[1]
            if item[0] in data:
                if data[item[0]]:
                    data[key_name] = data[item[0]]
                del data[item[0]]

    def drawSession(self, data, session_key):
        for key in list(data.get(session_key, {}).keys()):
            data.setdefault(key, data[session_key][key] \
                if data[session_key][key] and data[session_key][key] != "null" else "")
        if data.get(session_key, {}):
            del data[session_key]

    def addHead(self, data, exc_keys = []):
        for key in list(data.keys()):
            if key in exc_keys:
                continue
            if key.startswith(self.head):
                continue
            data.setdefault("".join([self.head, key]), data[key].strip() if type("")==type(data[key]) else data[key])
            del data[key]

    def addTs(self, data):
        key = "".join([self.head, "opTime"])
        if key not in data:
            return
        optm = data[key].replace(":", "").replace("-", "").replace(" ", "").replace("+", "")
        if len(optm) != 14:
            data.setdefault("".join([self.head, "opTime"]), 0)
            return
        data[key] = optm
        data.setdefault("".join([self.head, "ts"]), int(time.mktime(time.strptime(optm, "%Y%m%d%H%M%S"))*1000))


if __name__ == "__main__":
    import json
    import copy
    data = json.loads('''{"jhd_pb": "appstore", "jhd_vr": "2.0.2", "jhd_userkey": "63b3d5bd-64f1-44a5-9ae3-647a10dd7b6d", "jhd_os": "iphone_9.3.2", "jhd_session": {"jhd_pageName": "null", "jhd_netType": "wifi", "jhd_map": {"10": 121}, "jhd_interval": "null", "jhd_opTime": "2016-08-02+09:44:53", "jhd_opType": "action", "jhd_eventId": "ac22"}, "jhd_datatype": "feeling", "jhd_ua": "iphone8_1", "jhd_ip": "39.79.128.214"}''')
    # data = json.loads('''{"jhd_auth": "on", "jhd_ua": "lenovo k32c36", "jhd_map": "", "jhd_pushid": "UISEN127604D802F9208DA065IJ9E7SE", "jhd_opTime": "2016-08-01 16:51:29", "jhd_eventId": "wordroid.activitys.ReviewMain", "jhd_ip": "124.65.163.106", "jhd_pb": "Wandoujia", "jhd_userkey": "868524021023163", "jhd_os": "android 5.1.1", "jhd_region": "0.0,0.0", "jhd_opType": "page", "jhd_netType": "wifi", "jhd_sdk_version": "1.00", "jhd_vr": "1.0", "jhd_interval": "1", "jhd_datatype": "11FFE127604782539208DA065113107D"}''')
    tester = TransformRule()
    a = time.time()
    # data_tmp = copy.deepcopy(data)
    data_tmp = data
    print json.dumps(tester.applyRule(data))
    print time.time() - a



