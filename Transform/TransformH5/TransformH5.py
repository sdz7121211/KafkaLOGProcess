#-*- encoding: utf-8 -*-
import base64
import json
import urllib
import re
from Transform.Transform import Transform


class TransformH5(object):

    def __init__(self):
        self.ip_pattern = re.compile(
            r'''"((?:(?:25[0-5]|2[0-4]\d|((1\d{2})|([1-9]?\d)))\.){3}(?:25[0-5]|2[0-4]\d|((1\d{2})|([1-9]?\d))))"''')

    def transform(self, line):
        # if "params=" in line:
        #     data = line.split("params=")[1].split(" HTTP")[0]
        # elif "params%3d" in line:
        #     data = line.split("params=")[1].split(" HTTP")[0]
        # else:
        #     data = line.split("params=")[1].split(" HTTP")[0]

        params = line.split("params=")[1].split(" HTTP")[0]

        try:
            data = urllib.unquote(params)
            data = base64.b64decode(data)
            data = json.loads(data)
        except:
            params = line.split("params=")[1].split(" HTTP")[0]
            urlencode_guess = "unknown"
            if "%" not in params and "+" not in params:
                urlencode_guess = "no_urlencode"
            if "+" in params and "%" not in params:
                base64_guess = "yes_base64"
            if urlencode_guess != "no_urlencode":
                params = urllib.unquote(params)
            data = base64.b64decode(params)
            data = urllib.unquote(data)
            data = json.loads(data)

        # 如果为内网ip，做单独处理
        ip = line.split(",")[0].strip()
        try:
            if ip.startswith("127"):
                ip = self.ip_pattern.search(line).group(1)
        except:
            # import traceback
            # print(traceback.print_exc())
            ip = line.split(",")[0].strip()
        data["ip"] = ip
        # 从nigix日志中取出ua字段
        try:
            ua = line.split('"')[6]
            data.setdefault("ua", ua)
        except:
            import traceback
            print(traceback.print_exc())
        yield data


if __name__ == "__main__":
    line = '''127.0.0.1,-,[25/Nov/2016:17:38:01 +0800],"GET /h5sta.js?params=eyJhcHBrZXkiOiJuY2ZfaDUiLCJ0eXBlIjoiZHVyIiwic3RhdHVzIjoiZW5kIiwidmFsdWUiOjM3MSwidXJpIjoiaHR0cHM6Ly93d3cubmljYWlmdS5jb20vdHJhbnMjZmluZGJ1eXRhYiIsInVpZCI6IjE0ODAwNjU0MzgyMzJfMzJyeWszNjcyMSIsInRzIjoxNDgwMDY2Njc5OTE5LCJ2ciI6IjEuMy4yIiwiZGV2aWNlIjp7Im5hbWUiOiJwYyJ9LCJzeXN0ZW0iOnsibmFtZSI6IndpbmRvd3MiLCJ2ZXJzaW9uIjoiMTAuMCJ9LCJicm93c2VyIjp7Im5hbWUiOiJjaHJvbWUiLCJ2ZXJzaW9uIjoiNDkuMC4yNjIzLjExMCJ9LCJzY3JlZW4iOiIxMjgwKjk2MCIsInVhIjoiTW96aWxsYS81LjAgKFdpbmRvd3MgTlQgMTAuMDsgV09XNjQpIEFwcGxlV2ViS2l0LzUzNy4zNiAoS0hUTUwsIGxpa2UgR2Vja28pIENocm9tZS80OS4wLjI2MjMuMTEwIFNhZmFyaS81MzcuMzYifQ%3D%3D HTTP/1.1",-,200 13,"https://www.nicaifu.com/trans","Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.110 Safari/537.36" "123.115.228.197"'''
    tester = Transform()
    print tester.transform(line)