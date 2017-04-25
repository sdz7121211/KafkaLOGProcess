# -*- coding: utf-8 -*-
import time
from Common.DiskDict import DiskDict


class TransformRule(object):
    # 多线程支持
    try:
        dev_users_cache = DiskDict(cache_name="devusers")
    except:
        import traceback
        print traceback.print_exc()
        dev_users_cache = {}
    def __init__(self):
        self.head = "jhd_"
        self.lower_keys = ["".join([self.head, item]) for item in ["netType", "ua", "os"]]
        # self.rename_keys = {
        #     "jhd_opTime": "jhd_opTime",
        #     "jhd_location": "jhd_region"
        # }
        self.rename_keys = dict( [("".join([self.head, item[0]]), "".join([self.head, item[1]])) \
                                  for item in [("location", "region")]] )
        self.combine_keys = [("".join([self.head, item[0]]), "".join([self.head, item[1]])) \
                             for item in [("pageName", "eventId")]]
        self.devUsers = {}
        self.try_times = 1

    def applyRule(self, data):
        self.drawSession(data, "session")
        self.addHead(data)
        self.doLower(data)
        self.doRename(data)
        self.doCombine(data)
        self.addTs(data)
        self.judgeDevUser(data)
        return data

    def doLower(self, data):
        for key in self.lower_keys:
            if key not in data:
                continue
            data[key] = data[key].lower() if data[key] else "null"

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
            data.setdefault("".join([self.head, "opTime"]), int(time.time()) * 1000)
            return
        data[key] = optm
        try:
            data.setdefault("".join([self.head, "ts"]), int(time.mktime(time.strptime(optm, "%Y%m%d%H%M%S"))*1000))
        except:
            data.setdefault("".join([self.head, "ts"]), int(time.time()) * 1000)

    def judgeDevUser(self, data):
        try:
            datatype = data["jhd_datatype"]
            userkey = data["jhd_userkey"]
            if self.try_times > 0:
                url = '''http://123.59.147.152:21333/devusers/{"appkey":["all"],"iscache":true}'''
                import urllib
                import socket
                import json
                socket.setdefaulttimeout(2)
                result = urllib.urlopen(url).read()
                devUsers_raw = json.loads(result)
                # unicode转化为str，兼容python2.6版本，该版本中 DiskDict的key仅支持string、int类型
                for key in devUsers_raw:
                    if isinstance(key, unicode):
                        self.devUsers[key.encode('utf-8')] = map(lambda item: item.encode("utf-8"), devUsers_raw[key])
                    else:
                        self.devUsers[key] = devUsers_raw[key]
                self.try_times = self.try_times - 1
                self.__class__.dev_users_cache.update(self.devUsers)
            if userkey in self.devUsers.get(datatype, set()):
                data["isdevuser"] = True
        except:
            import traceback
            print traceback.print_exc()
            print json.dumps(data)
            # print(json.dumps([u"@%s,过滤测试用户出错！" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())),), traceback.print_exc(), data]))


if __name__ == "__main__":
    import json
    import copy
    data = json.loads('''{"datatype": "11FFE127604782539208DA065113107D", "ua": "Lenovo K32c36", "vr": "1.0", "auth": "on", "pb": "Wandoujia", "sdk_version": "1.00", "session": {"eventId": "null", "map": null, "opType": "page", "opTime": "2016-08-01 16:51:29", "interval": "1", "netType": "WIFI", "pageName": "wordroid.activitys.ReviewMain"}, "location": "0.0,0.0", "ip": "124.65.163.106", "userkey": "868524021023163", "os": "Android 5.1.1", "pushid": "UISEN127604D802F9208DA065IJ9E7SE"}''')
    # data = json.loads('''{"jhd_auth": "on", "jhd_ua": "lenovo k32c36", "jhd_map": "", "jhd_pushid": "UISEN127604D802F9208DA065IJ9E7SE", "jhd_opTime": "2016-08-01 16:51:29", "jhd_eventId": "wordroid.activitys.ReviewMain", "jhd_ip": "124.65.163.106", "jhd_pb": "Wandoujia", "jhd_userkey": "868524021023163", "jhd_os": "android 5.1.1", "jhd_region": "0.0,0.0", "jhd_opType": "page", "jhd_netType": "wifi", "jhd_sdk_version": "1.00", "jhd_vr": "1.0", "jhd_interval": "1", "jhd_datatype": "11FFE127604782539208DA065113107D"}''')
    tester = TransformRule()
    a = time.time()
    tester.judgeDevUser({"jhd_datatype": "caiyu_ad", "jhd_userkey": "uid"})
    # data_tmp = copy.deepcopy(data)
    # data_tmp = data
    # print(json.dumps(tester.applyRule(data)))
    print(time.time() - a)



