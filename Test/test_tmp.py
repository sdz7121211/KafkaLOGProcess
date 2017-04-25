# -*- coding: utf-8 -*-


import traceback
import json
import sys

exc_type, exc_value, exc_traceback = sys.exc_info()
errinfo = traceback.format_exception(exc_type, exc_value, exc_traceback)
print errinfo