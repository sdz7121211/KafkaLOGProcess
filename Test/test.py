# -*- coding: utf-8 -*-
import sys



class testyielddict(object):


    def yielddict(self):
        yield


    def funcargs(self, *args, **kwargs):
        print "args", args
        print "kwargs", kwargs
        print("222222222")

    def testargs(self, *args, **kwargs):
        print "testargs args", args
        print "testargs kwargs", kwargs
        def inner_func(*_args, **_kwargs):
            print list(args) + list(_args)
            print kwargs, _kwargs, kwargs.update(_kwargs)
            return self.funcargs(*(list(args) + list(_args)), **(kwargs.update(_kwargs)))
        return inner_func
        # return lambda *_args, **_kwargs: self.funcargs(*(list(args) + list(_args)), **(kwargs.update(_kwargs)))



if __name__ == "__main__":

    tester = testyielddict()
    tester.testargs(555, path = '/test')(666)
    # tester.testargs(555)()
    # for i in tester.yielddict():
    #     print("666", i)
    #     print i