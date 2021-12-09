import sys
import os
sys.path.append(os.path.dirname(os.path.realpath(__file__))+"/../")
from library.config import config
from library.mysql import mysql

cfgTS=config.getConfig('ts')
db=cfgTS['db']

tables_list=mysql.selectToList('show tables',db)
for v in tables_list:
    table=list(v.values())[0]
    index_list=['ts_code','end_date','trade_date']
    for index in index_list:
        sql="CREATE INDEX "+index+" ON "+table+" ("+index+"(10)) "
        mysql.exec(sql,db)

 
