# -*- coding: utf-8 -*-


class InnerFunc(object):

    def __init__(self):
        pass

    def outer_func(self):

        print "i am outer_func."

        def inner_func():
            print "i am inner_func."

if __name__ == "__main__":
    tester = InnerFunc()

    tester.outer_func()
    tester.outer_func.inner_func
