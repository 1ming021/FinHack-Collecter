import sys
import datetime
from library.config import config
from library.mysql import mysql
from collect.ts.helper import tsSHelper
from library.monitor import tsMonitor
import time
import pandas as pd
import traceback
from library.alert import alert

class tsFund:
    @tsMonitor
    def fund_basic(pro,db):
        table='fund_basic'
        mysql.truncateTable(table,db)
        engine=mysql.getDBEngine(db)
        data=pro.fund_basic(market='E',status='D')
        data.to_sql(table, engine, index=False, if_exists='append', chunksize=5000)
        data=pro.fund_basic(market='E',status='I')
        data.to_sql(table, engine, index=False, if_exists='append', chunksize=5000)
        data=pro.fund_basic(market='E',status='L')
        data.to_sql(table, engine, index=False, if_exists='append', chunksize=5000)
        data=pro.fund_basic(market='O',status='D')
        data.to_sql(table, engine, index=False, if_exists='append', chunksize=5000)
        data=pro.fund_basic(market='O',status='I')
        data.to_sql(table, engine, index=False, if_exists='append', chunksize=5000)
        data=pro.fund_basic(market='O',status='L')
        data.to_sql(table, engine, index=False, if_exists='append', chunksize=5000)
    
    @tsMonitor
    def fund_company(pro,db):
        tsSHelper.getDataAndReplace(pro,'fund_company','fund_company',db)
    
    @tsMonitor
    def fund_manager(pro,db):
        mysql.truncateTable('fund_manager',db)
        engine=mysql.getDBEngine(db)
        data=tsSHelper.getAllFund(db)
        fund_list=data['ts_code'].tolist()
        
        for i in range(0,len(fund_list),100):
            code_list=fund_list[i:i+100]
            while True:
                try:
                    df = pro.fund_manager(ts_code=','.join(code_list))
                    df.to_sql('fund_manager', engine, index=False, if_exists='append', chunksize=5000)
                    break
                except Exception as e:
                    if "最多访问" in str(e):
                        print(self.func.__name__+":触发限流，等待重试。\n"+str(e))
                        time.sleep(15)
                        continue
                    else:
                        info = traceback.format_exc()
                        alert.send('fund_manager','函数异常',str(info))
                        print(info)
                        break
    
    @tsMonitor
    def fund_share(pro,db):
        data=tsSHelper.getAllFund(db)
        fund_list=data['ts_code'].tolist()
        for ts_code in fund_list:
            tsSHelper.getDataWithLastDate(pro,'fund_share','fund_share',db,'trade_date',ts_code)
    
    @tsMonitor
    def fund_nav(pro,db):
        tsSHelper.getDataWithLastDate(pro,'fund_nav','fund_nav',db,'nav_date')
    
    @tsMonitor
    def fund_div(pro,db):
        tsSHelper.getDataWithLastDate(pro,'fund_div','fund_div',db,'ann_date')
    
    @tsMonitor
    def fund_portfolio(pro,db):
        tsSHelper.getDataWithLastDate(pro,'fund_portfolio','fund_portfolio',db,'ann_date')
    
    @tsMonitor
    def fund_daily(pro,db):
        tsSHelper.getDataWithLastDate(pro,'fund_daily','fund_daily',db)
    
    @tsMonitor
    def fund_adj(pro,db):
        tsSHelper.getDataWithLastDate(pro,'fund_adj','fund_adj',db)