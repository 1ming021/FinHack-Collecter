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
from collect.ts.helper import tsSHelper

class tsAStockIndex:

    @tsMonitor
    def index_basic(pro,db):
        tsSHelper.getDataAndReplace(pro,'index_basic','astock_index_basic',db)
    
    @tsMonitor
    def index_daily(pro,db):
        data=tsSHelper.getAllAStockIndex(pro,db)
        index_list=data['ts_code'].tolist()
        for ts_code in index_list:
            lastdate=tsSHelper.getLastDateAndDelete('astock_index_daily','trade_date',ts_code=ts_code,db=db)
            engine=mysql.getDBEngine(db)   
            today = datetime.datetime.now()
            today=today.strftime("%Y%m%d")
            while True:
                try:
                    df=pro.index_daily(ts_code=ts_code, start_date=lastdate, end_date=today)
                    if(not df.empty):
                        res = df.to_sql('astock_index_daily', engine, index=False, if_exists='append', chunksize=5000)
                    break
                except Exception as e:
                    if "最多访问" in str(e):
                        print('index_daily'+":触发限流，等待重试。\n"+str(e))
                        time.sleep(15)
                        continue
                    else:
                        info = traceback.format_exc()
                        alert.send('index_daily','函数异常',str(info))
                        
                        print('index_daily'+"\n"+info)
                        break                
                
                
            #tsSHelper.getDataWithLastDate(pro=pro,api='index_daily',table='astock_index_daily',db=db,ts_code=ts_code)
    
    @tsMonitor
    def index_weekly(pro,db):
        tsSHelper.getDataWithLastDate(pro,'index_weekly','astock_index_weekly',db)
    
    @tsMonitor
    def index_monthly(pro,db):
        tsSHelper.getDataWithLastDate(pro,'index_monthly','astock_index_monthly',db)
    
    @tsMonitor
    def index_weight(pro,db):
        tsSHelper.getDataWithLastDate(pro,'index_weight','astock_index_weight',db)
    
    @tsMonitor
    def index_dailybasic(pro,db):
        tsSHelper.getDataWithLastDate(pro,'index_dailybasic','astock_index_dailybasic',db)
    
    @tsMonitor
    def index_classify(pro,db):
        tsSHelper.getDataAndReplace(pro,'index_classify','astock_index_classify',db)
    
    @tsMonitor
    def index_member(pro,db):
        tsSHelper.getDataWithCodeAndClear(pro,'index_member','astock_index_member',db)
        pass
    
    @tsMonitor
    def daily_info(pro,db):
        tsSHelper.getDataWithLastDate(pro,'daily_info','astock_index_daily_info',db)
    
    @tsMonitor
    def sz_daily_info(pro,db):
        tsSHelper.getDataWithLastDate(pro,'sz_daily_info','astock_index_sz_daily_info',db)
    
    @tsMonitor
    def ths_daily(pro,db):
        pass
        #tsSHelper.getDataWithLastDate(pro,'ths_daily','astock_index_ths_daily',db)
    