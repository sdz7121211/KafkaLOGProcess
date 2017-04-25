# coding: utf-8
import sys
import json
import urllib
import re

from Decipher import Decipher
from TransformRule import TransformRule
from Transform.Transform import Transform

reload(sys)
sys.setdefaultencoding("utf-8")


class TransformSaaS(Transform):

    def __init__(self):
        self.decipher = Decipher()
        self.params_format = re.compile(unicode(r"\[.*]"))
        self.ip_pattern = re.compile(
            r'''"((?:(?:25[0-5]|2[0-4]\d|((1\d{2})|([1-9]?\d)))\.){3}(?:25[0-5]|2[0-4]\d|((1\d{2})|([1-9]?\d))))"''')
        self.rule = TransformRule()

    def deformation(self, *args, **kwargs):
        pass

    def retransform(self, *args, **kwargs):
        pass

    def transform(self, line):
        line = urllib.unquote_plus(line).decode("utf8")
        items = line.split(",")
        ip = items[0]
        # 如果为内网ip，做单独处理
        try:
            if ip.startswith("127"):
                ip = self.ip_pattern.search(line).group(1)
        except:
            pass
        response_ty = items[3]
        assert "post" in response_ty.lower(), 'isnot POST REQUEST'
        content = items[4]
        if "&params=" in content:
            p1, p2 = content.split("&")
            tipkey = p1.split("abc=")[1].strip()
            params_enauth = p2.split("params=")[1].strip()
            params = self._authLog(tipkey, params_enauth)
            auth_sw = 'on'
        else:
            params = urllib.unquote_plus(content.split("=")[1]).decode("utf8")
            auth_sw = 'off'
        for item in self.params_format.findall(params):
            params = item
            break
        for item in json.loads(params):
            item['auth'] = auth_sw
            item['ip'] = ip
            yield self.rule.applyRule(item)

    def _authLog(self, tipkey, params):
        dtipkey = self.decipher.deCiphering(tipkey)
        dparams = self.decipher.deCiphering(params, dtipkey)
        return dparams

if __name__ == "__main__":
    tester_Transform = TransformSaaS()
    line = '''223.73.214.240,-,[15/Dec/2016:16:10:04 +0800],\"POST /appsta.js HTTP/1.1\",abc=7A2EANoWnTZHBOe1xQInyw%3D%3D&params=GYHaQuxPLurPBC6snxUAyM3T2iNzTOtK%2BxZGVFXweV1zixxm21RUF1iUX6%2Bi+9P9IBBlvYKDfpajOZgaKYz61xogBhlIDXZu3ogVWVL9EOAz8OAnzfhzPRhjI+DEVPmTKCeyrFu6xNxBSlnZwJVuDZJ5LUUffzHR4%2Fwk51nt1FOlGWYDx%2Fv%2FuB+6mM1GuwrQP9n4bb8s9vqUWj44xHEJZr%2Bzye6sDpJytgVmUe6Jig2kZ0QbZ4M+c7YGLZ9vpH3LJ2jkCEOCwkEgl%2Bu1yllXhBIqUqSFeEuWfmFywHuKpz2bB4HX+lYv%2Bc6ekQj5Am5YJccj67zKLmYD6du3hdful3KkFgEyUQKxefPTxFuzmMEnp+YzWEq07Wd68rCaYcRIUeFFdmlakpXUFC5bBxiXa2IWQBDHzvu5Z7pQVlSurU+e12tizcyIdcRP9urCrEHgB7XjLzMlDmeVuTBKXqQ6mIbQDSI%2FykZFkvBCi4j+yhfCy6IYnrAaw5hdUC12qBV33rVWEfywgqbfKfWsk64Ft62zksZszaAn44b%2F+1Iy19ywwdV3YjyA%3D,200 18,\"-\",\"Dalvik/2.1.0 (Linux; U; Android 5.1.1; MiBOX3 Build/LMY49J)\" \"-\"'''
    for item in tester_Transform.transform(line):
        print("eeeee", item)