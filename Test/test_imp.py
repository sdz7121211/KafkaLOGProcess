# -*- coding: utf-8 -*-
from os import sys, path
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
print(path.dirname(path.dirname(path.abspath(__file__))))
import imp
import importlib
m1 = importlib.import_module("Factory.TransformSaaSFactory", "TransformSaaSFactory")
aclass = getattr(m1, "TransformSaaSFactory")
p = aclass()
print(dir(p))
# p = new.instance(aClass)
print(m1, p, type(p.create()), p.create())
# import

# stringmodule = imp.load_module('transform_factory', , "")

# print(staticmethod)