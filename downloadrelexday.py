import urllib3
import json
import calendar
from datetime import *
from dateutil.relativedelta import *

url = "http://api.goseek.cn/Tools/holiday"
http = urllib3.PoolManager()
today = date.today()
datelist = []
for i in range(0, 365 * 2):
    datelist.append(today)
    today = today + relativedelta(days=-1)

file = open('relexdatalist.txt','w')
for date in datelist:
    r = http.request('get','http://api.goseek.cn/Tools/holiday', fields={'date':date.strftime("%Y-%m-%d")})
    text = json.loads(r.data.decode())
    if text['data'] > 0:
        file.write(date.strftime("%Y-%m-%d\n"))

file.close()
