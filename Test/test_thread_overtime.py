# -*- coding: utf-8 -*-
from threading import Thread
import time
import sys
import logging

logger = logging.getLogger(__file__)

class TimeoutException(Exception):
    pass

ThreadStop = Thread._Thread__stop # 获取私有函数


def timelimited(timeout):
    def decorator(function):
        def decorator2(*args, **kwargs):
            class TimeLimited(Thread):
                def __init__(self, _error = None,):
                    Thread.__init__(self)
                    self._error =  _error

                def run(self):
                    try:
                        print(function)
                        self.result = function(*args, **kwargs)
                    except Exception, e:
                        logger.error(sys.exc_info())
                        self._error = e

                def _stop(self):
                    if self.isAlive():
                        ThreadStop(self)

            t = TimeLimited()
            t.start()
            t.join(timeout)

            if isinstance(t._error, TimeoutException):
                t._stop()
                raise TimeoutException('timeout for %s' % (repr(function)))

            if t.isAlive():
                t._stop()
                raise TimeoutException('timeout for %s' % (repr(function)))
            if t._error is None:
                return t.result
        return decorator2
    return decorator

@timelimited(6)
def fn_1(secs):
    time.sleep(secs)
    print("执行完成")
    return 'Finished'

if __name__ == "__main__":
     _thread = Thread(target=fn_1, args=(4,))
     try:
        _thread.start()
     except:
        print("超时退出")

     a = time.time()
     while True:
         print("costs: %.2f" % (time.time() - a,))
         time.sleep(1)
