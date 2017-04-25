# -*- coding: utf-8 -*-

import ConfigParser

cf = ConfigParser.ConfigParser()
# read config
cf.read("test.conf")
# return all section
secs = cf.sections()
print 'sections:', secs

opts = cf.options("sec_a")
print 'options:', opts

kvs = cf.items("sec_a")
print 'sec_a:', kvs

# read by type
str_val = cf.get("sec_a", "a_key1")
int_val = cf.getint("sec_a", "a_key2")

print "value for sec_a's a_key1:", str_val
print "value for sec_a's a_key2:", int_val

# write config
# update value
cf.set("sec_b", "b_key3", "new-$r")
# set a new value
cf.set("sec_b", "b_newkey", "new-value")
# create a new section
cf.add_section('a_new_section')
cf.set('a_new_section', 'new_key', 'new_value')

# write back to configure file
cf.write(open("test.conf", "w"))