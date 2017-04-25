#-*- encoding: utf-8 -*-
import base64
from Crypto.Cipher import AES
import re


class Decipher(object):

    def __init__(self):
        self.iv = "*"
        self.mode = re.compile(r"(\w|\W)+")

    def deCiphering(self, log, keyStr = None):
        # log = urllib.unquote_plus(log).decode("utf8")
        keySpec = "".join(["jh_", keyStr]) if keyStr else "*"
        decode_result = base64.b64decode(log)
        cryptor = AES.new(keySpec, AES.MODE_CBC, self.iv)
        decryptlog = cryptor.decrypt(decode_result)
        # decryptlog = decryptlog[:len(decryptlog)-5] if keyStr else decryptlog[:13]
        # 去除EOT字符
        decryptlog = decryptlog.rstrip("\04") if keyStr else decryptlog[:13]
        return decryptlog


if __name__ == "__main__":
    test_Decipher = Decipher()
    a = test_Decipher.deCiphering('usNvQ013R5eZu4rAfKdsbw%3D%3D')
    # print(a)
    # print(test_Decipher.deCiphering("Oh/s7tw54Q6rwbTEA2LxUzA1/j+L/zFaioODg0qYX5LJRtLH/fvfNtgj52T1 jpD+Ics0hydn1tHnE4MbHdqwwdHAzoklhOvAT9SW6DI5QRFPXA9xUwtV15Pw 6+N27PeDM9L9tOhZ+j/KVN52wUxp7Idqj0UesuIdNfp8CvpJ6+jHidv3iKI4 L+U4bxqkW39HzmZgX96WG3oBVZavKWlMfjS6/Y2gEQ/3TXto+VwnCGEiY/zd WB5TtFK7U8xHC+wJGXik/TTIesOgvLDFVxdjaaf4l+s+thwq251SuT6T/IXu dk2/Yp5Ckmw0aRLZnENk7M6m6dsepCKWf0Mz3vZNUosMLnOi6vaI5/xXd2jQ +vIqM2DjdcQAwM7G39zxHxqcv7rvz4poFvmFgsA4dHz4xuuc814bCOl/6dGT f+lwCDt3CoSRZbGyBC7Kc4guraBuNJ1Bs30LDlGH8QRDj++UdT3YLe3DiI2P poZXguZU02rE+m7LEnaMq1br1WYzXR5DCtWYr4dSB2SYnxhSh9wddDPydBhB bmYjhp/Slswuk0YpqVt8RwyU/tJyy9JA8Mbdjm6h4PQjbJ+x9IUfmSVPGzSp bqMNppEYrvWA4HIY+wB1BuuGlm+SFj+44vA2eUKrg4n1roFryfKFjYBfmvVN 3q3dART4qIinfErlIUKjGW8JXO9QHPv27laFHYelFWsPIWe9sHWmuNpfKJQH SR9xdhxQJBiAH/pHrQmcaCrDJxzb5jsWZ6Qjgx05/ww4l4b/IkcILsbawQVN Nk//KQ+S2Pq5lVAX32eVlBYRWHV6QPLnYVHm87fu6LVYrurcxzUlDYSRR/Ye lK1RHOi6VBQ5D9hRNw/lL36vxq7FBfwU9vvLl6rtQ7HRgQb7JVEDPsRDx5nY jhOJ8jNe82od7gl4kMtDmYKXxpvlJZbv05T0Pgp9vBLZGKnO+4l2Mprjy3Zb", '1464514969589'))
    # print(test_Decipher.deCiphering("ByCuI1%2BHKrMJRo5Pfa6JX33YwP7YxQI23xqML37pgIgDMIjpklQkRyj6UyrD+awDJI%2FtbBY3gsu9SdCSoxeu%2B5bJ2hFn1t43J4Jss7cVveC6YIQH5s%2FO7r2WN+3QqphHriKecHSMnd%2B%2FDOSDc%2BxvcGs5VryTEIEdCHpNIflMx5OFPW1QNxvCQZ+C1FrF9Q%2FaI6XKmUFvnprAK%2Fgd29nYVksypvgsZN%2BzuLMLP0Lwes249d0qxYA+rnrtjpq%2BzTPj7kx88osxDXM5CuGxyPL7k9S8E2YKdVDAr5EeGHyo%2BWuU8xvp+fvwMHxn9gskaZIlu7%2BiD3p8k%2B09eEkvMfdehiIv3DfbMr9moUv1wQDyCCjNk+7KwWU85PUioSi5SJVIz0AM7Ig5JWfeoxYri%2BilraM8T0GyN7GnQFkJMzKJd4+pHO48%2FB%2BQX9cZRbGKfyWJScOFM9XH0SsFhoov44vI5uyF4M0JotuQwGA1mg4+JCeUjLkhuHesDbQ2EdH8QUivCjb8OLgEyS%2FGbp8kDkzYXHQOsofC7iNbknul+KtGvOf2IXtc%2BywI%3D", a))
