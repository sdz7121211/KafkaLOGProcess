# coding: utf-8
# import marshal
from os import path
import os
import uuid
import time
import bsddb
import threading
try:
    import cPickle as pickle
except ImportError:
    import pickle

__author__ = "sudz"
__email__ = "670108918@qq.com"

global cacheNames

cacheNames = {}


class DiskDict:

    __setitem__mutex__ = {}

    def __init__(*args, **kwargs):

        if not args:
            raise TypeError("descriptor '__init__' of 'DiskDict' object "
                            "needs an argument")
        self = args[0]
        cacheNames[id(self)] = kwargs.pop("cache_name", None)
        self._initcache(cachefile_name=cacheNames[id(self)])
        args = args[1:]
        if len(args) > 1:
            raise TypeError('expected at most 1 arguments, got %d' % len(args))
        if args:
            dict = args[0]
        elif 'dict' in kwargs:
            dict = kwargs.pop('dict')
            import warnings
            warnings.warn("Passing 'dict' as keyword argument is "
                          "deprecated", PendingDeprecationWarning,
                          stacklevel=2)
        else:
            dict = None

        if dict is not None:
            self.update(dict)
        if len(kwargs):
            self.update(kwargs)

    def _initcache(self, cachefile_name = None):

        if cachefile_name:
            self._id = cachefile_name
        else:
            self._id = "".join([uuid.uuid4().hex, time.strftime("%Y%m%d%H%M%S")])
        self._cache_path = path.dirname(__file__)
        if len(self._cache_path) == 0:
            self._cache_path = os.getcwd()
        assert path.exists(self._cache_path)
        if cachefile_name:
            if "/".join([self._cache_path, self._id]) not in self.__class__.__setitem__mutex__:
                self.data = bsddb.btopen("/".join([self._cache_path, self._id]), "c")
                self.__class__.__setitem__mutex__.setdefault("/".join([self._cache_path, self._id]), [threading.Lock(), self.data, 0])
                self.__class__.__setitem__mutex__["/".join([self._cache_path, self._id])][2] += 1
            else:
                self.data = self.__class__.__setitem__mutex__["/".join([self._cache_path, self._id])][1]
                self.__class__.__setitem__mutex__["/".join([self._cache_path, self._id])][2] += 1
        else:
            self.data = bsddb.btopen("/".join([self._cache_path, self._id]), "n")
            self.__class__.__setitem__mutex__.setdefault("/".join([self._cache_path, self._id]), [threading.Lock(), self.data, 0])

    def __repr__(self):
        return repr(dict(zip(self.keys(), self.values())))

    def __cmp__(self, dict):
        if isinstance(dict, DiskDict):
            return cmp(self.data, dict.data)
        else:
            return cmp(self.data, dict)

    __hash__ = None  # Avoid Py3k warning

    def __len__(self):
        return len(self.data)

    def __getitem__(self, key):
        _key = pickle.dumps(key, 2)
        if self.data.has_key(_key):
            return pickle.loads(self.data[_key])
        if hasattr(self.__class__, "__missing__"):
            return self.__class__.__missing__(self, key)
        raise KeyError(_key)

    def __setitem__(self, key, item):
        # print len(self.__class__.__setitem__mutex__), self.__class__.__setitem__mutex__
        _key = pickle.dumps(key, 2)
        locker = self.__class__.__setitem__mutex__["/".join([self._cache_path, self._id])][0] if "/".join([self._cache_path, self._id]) in self.__class__.__setitem__mutex__ else None
        locker.acquire()
        self.data[_key] = pickle.dumps(item, 2)
        locker.release()


    def __delitem__(self, key):
        _key = pickle.dumps(key, 2)
        if not self.data.has_key(_key):
            raise KeyError(key)
        del self.data[_key]

    def __del__(self):
        if hasattr(self, 'data'):
            self.data.close()
            if cacheNames[id(self)] is None:
                os.remove(os.path.join(self._cache_path, self._id))

    def __contains__(self, key):
        _key = pickle.dumps(key, 2)
        return self.data.has_key(_key)

    def __iter__(self):
        for key in self.iterkeys():
            yield key

    def clear(self):
        if hasattr(self, 'data'):
            self.data.close()
            self.data = bsddb.btopen(os.path.join(self._cache_path, self._id), "n")

    def close(self):
        if hasattr(self, "data"):
            self.data.close()

    def copy(self):
        raise AttributeError

    def keys(self):
        return map(pickle.loads, self.data.keys())

    def values(self):
        return map(pickle.loads, self.data.values())

    def items(self):
        return zip(self.keys(), self.values())

    def iteritems(self):
        for k, v in self.data.iteritems():
            yield (pickle.loads(k), pickle.loads(v))

    def iterkeys(self):
        for k, v in self.data.iteritems():
            yield pickle.loads(k)

    def itervalues(self):
        for k, v in self.data.iteritems():
            yield pickle.loads(v)

    def has_key(self, key):
        _key = pickle.dumps(key, 2)
        return self.data.has_key(_key)

    def update(*args, **kwargs):
        if not args:
            raise TypeError("descriptor 'update' of 'DiskDict' object "
                            "needs an argument")
        self = args[0]
        args = args[1:]
        if len(args) > 1:
            raise TypeError('expected at most 1 arguments, got %d' % len(args))

        if args:
            dict = args[0] # set DiskDict/dict/bsddb as dict
        elif 'dict' in kwargs:
            dict = kwargs.pop('dict')
            import warnings
            warnings.warn("Passing 'dict' as keyword argument is deprecated",
                          PendingDeprecationWarning, stacklevel=2)
        else:
            dict = None

        if dict is None:
            pass
        elif isinstance(dict, DiskDict):
            if id(dict) != id(self):
                for k, v in dict.iteritems():
                    self.data[pickle.dumps(k)] = pickle.dumps(v)
        elif isinstance(dict, type({})):
            for k, v in dict.iteritems():
                self[k] = v
        if len(kwargs):
            self.update(kwargs)

    def get(self, key, failobj=None):
        _key = pickle.dumps(key, 2)
        if not self.data.has_key(_key):
            return failobj
        return pickle.loads(self.data[_key])

    def setdefault(self, key, failobj=None):
        _key = pickle.dumps(key, 2)
        if _key not in self.data:
            self.data[_key] = pickle.dumps(failobj, 2)
        return pickle.loads(self.data[_key])

    def pop(self, key, *args):
        _key = pickle.dumps(key, 2)
        if not self.data.has_key(_key):
            raise KeyError(key)
        value = self.data[_key]
        del self.data[_key]
        return pickle.loads(value)

    def popitem(self):
        key, value = None, None
        for k, v in self.data.iteritems():
            key, value = k, v
            break
        del self.data[key]
        return (pickle.loads(key), pickle.loads(value))

    @classmethod
    def fromkeys(cls, iterable, value=None):
        d = cls()
        for key in iterable:
            _key = pickle.dumps(key, 2)
            d[_key] = pickle.dumps(value, 2)
        return d



class testclass(object):

    def __init__(self, value):
        self.value = value

    def setValue(self, value):
        self.value = value


if __name__ == "__main__":
    ######### TEST #########
    # null_dict = DiskDict()
    # time.sleep(10)
    # tester = DiskDict(cache_name="test_diskdict666")
    tester = DiskDict(cache_name="devusers")
    # tester["666"] = ['1480220823123_aor7ys9063'*1]
    print tester

    # for key in tester:
    #     print(key)
    # tester_1 = DiskDict({"a": "d", "b": "f"})
    # print("__str__", tester_1)
    # tester.update(tester_1)
    # print(tester.items())
    # tester.update({"a": "c"})
    # tester["test"] = "testdddd"
    # tester["test"] = "test2222"
    # print(tester.setdefault("test", "aaaaa"))
    # print(tester["test"])
    # print(tester.setdefault("test", set([1, 3, 4])))
    # print(tester.setdefault("test1", "aaaaa"))
    # print(tester.get("test", "sadfasfsadf"))
    # print("test" in tester)
    # print("testssss" in tester)
    # print(tester.keys())
    # print(tester.values())
    # print(tester.items())
    #
    # tester[1] = 2
    # print(tester.popitem())
    # tester["pop"] = "opopopo"
    # print(tester.pop("pop"))
    # print(tester.get("pop", "nopop"))
    # print(len(tester))
    #
    # print("ssssssss", tester.keys())
    # print("dddddddd", tester.values())
    # print("a" in tester)
    # print("b" in tester)
    # print([key for key in tester])
    # for key in tester:
    #     print("key", key)
    #
    # tester.clear()
    # print(len(tester))
    # # del tester["test1"]
    #
    # print(list(tester.iterkeys()))
    # print(list(tester.itervalues()))
    #
    #
    # # test cmp
    # test_1 = DiskDict()
    # test_1[1] = 2
    # test_2 = DiskDict()
    # test_2[1] = 2
    # print(test_1 == test_2)
    #
    # i = testclass("sdfasfsa")
    #
    # # j = pickle.dumps(i)
    # print(pickle.dumps(i, 2))
    # tester["1"] = i
    #
    #
    # print(tester["1"].value)
    # print(id(i))
    # print(id(tester["1"]))
    # print(tester["1"].value)
    # tester["1"].setValue("11111")
    # print(tester["1"].value)


