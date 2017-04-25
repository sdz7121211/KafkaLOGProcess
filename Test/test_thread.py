# -*- coding: utf-8 -*-
import threading
import time


class testThread(object):

    def __init__(self):
        _thread = threading.Thread(name="test", target=self.myrun)
        _thread.start()

    def myrun(self):
        i = 1
        while True:
            print("run %d" % (i,))
            i += 1
            time.sleep(2)


if __name__ == "__main__":
    tester = testThread()
    time.sleep(100)
