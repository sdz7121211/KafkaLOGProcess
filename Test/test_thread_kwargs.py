# -*- coding: utf-8 -*-
import threading
import time


class aclass(object):

    def __init__(self):
        self.records = {}

def func1(infos):
    while True:
        print(infos)
        time.sleep(2)

def func3(infos):
    import random
    while True:
        infos.setdefault(random.randint(1, 666), random.randint(1, 666))
        time.sleep(1)

def func2():

    obj = aclass()
    t1 = threading.Thread(target=func1, args=(obj.records,))
    t1.start()
    t2 = threading.Thread(target=func3, args=(obj.records,))
    t2.start()
    while True:
        time.sleep(2)

if __name__ == "__main__":
    func2()