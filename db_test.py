__author__ = 'multiangle'
import File_Interface as FI
from DB_Interface import MySQL_Interface
import json
import re
import urllib.request as request
import urllib.parse as parse
import time
import datetime
import redis
import random

dbi=MySQL_Interface()
print(dbi.get_line_num('ready_to_get'))


# print(invalid_data)
# FI.save_pickle(invalid_data,'test.pkl')

# data=FI.load_pickle('test.pkl')
# print(data)

