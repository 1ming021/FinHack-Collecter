import sys
sys.path.append("..")
sys.path.append("../..")
import datetime
from library.config import config
from library.mysql import mysql
from collect.ts.helper import tsSHelper
from library.monitor import tsMonitor
import time
import traceback
from library.alert import alert

class tsAStockPrice:
    def getPrice(pro,api,table,db):
        engine=mysql.getDBEngine(db)
        lastdate=tsSHelper.getLastDateAndDelete(table=table,filed='trade_date',ts_code="",db=db)
        begin = datetime.datetime.strptime(lastdate, "%Y%m%d")
        end = datetime.datetime.now()
        i=0
        while i<(end - begin).days+1:
            day = begin + datetime.timedelta(days=i)
            day=day.strftime("%Y%m%d")
            f = getattr(pro, api)
            while True:
                try:
                    df=f(trade_date=day)
                    break
                except Exception as e:
                    if "最多访问" in str(e):
                        print(api+":触发限流，等待重试。\n"+str(e))
                        time.sleep(15)
                        continue
                    elif "您没有访问该接口的权限" in str(e):
                        break
                    else:
                        info = traceback.format_exc()
                        alert.send(api,'函数异常',str(info))
                        print(info)
                        break
                    break
            #print(table+'-'+str(len(df))+'-'+day)
            res = df.to_sql(table, engine, index=False, if_exists='append', chunksize=5000)
            i=i+1
     
    
    @tsMonitor
    def daily(pro,db):
        tsAStockPrice.getPrice(pro,'daily','astock_price_daily',db)

    @tsMonitor
    def weekly(pro,db):
        tsAStockPrice.getPrice(pro,'weekly','astock_price_weekly',db)

    
    @tsMonitor
    def monthly(pro,db):
        tsAStockPrice.getPrice(pro,'monthly','astock_price_monthly',db)
    
    # @tsMonitor
    # def pro_bar(pro,db):
    #     tsStockPrice.getPrice(pro,'daily','astock_price_daily',db)
    
    @tsMonitor
    def adj_factor(pro,db):
        tsAStockPrice.getPrice(pro,'adj_factor','astock_price_adj_factor',db)
    
    @tsMonitor
    def suspend_d(pro,db):
        tsAStockPrice.getPrice(pro,'suspend_d','astock_price_suspend_d',db)
    
    @tsMonitor
    def daily_basic(pro,db):
        tsAStockPrice.getPrice(pro,'daily_basic','astock_price_daily_basic',db)
    
    @tsMonitor
    def moneyflow(pro,db):
        tsAStockPrice.getPrice(pro,'moneyflow','astock_price_moneyflow',db)
    
    @tsMonitor
    def stk_limit(pro,db):
        tsAStockPrice.getPrice(pro,'stk_limit','astock_price_stk_limit',db)
    
    @tsMonitor
    def limit_list(pro,db):
        tsAStockPrice.getPrice(pro,'limit_list','astock_price_limit_list',db)
    
    @tsMonitor
    def moneyflow_hsgt(pro,db):
        tsAStockPrice.getPrice(pro,'moneyflow_hsgt','astock_price_moneyflow_hsgt',db)
    
    @tsMonitor
    def hsgt_top10(pro,db):
        tsAStockPrice.getPrice(pro,'hsgt_top10','astock_price_hsgt_top10',db)
    
    @tsMonitor
    def ggt_top10(pro,db):
        tsAStockPrice.getPrice(pro,'ggt_top10','astock_price_ggt_top10',db)
    
    @tsMonitor
    def hk_hold(pro,db):
        tsAStockPrice.getPrice(pro,'hk_hold','astock_price_hk_hold',db)
    
    @tsMonitor
    def ggt_daily(pro,db):
        mysql.truncateTable('astock_price_ggt_daily',db)
        engine=mysql.getDBEngine(db)
        data = pro.ggt_daily()
        data.to_sql('astock_price_ggt_daily', engine, index=False, if_exists='append', chunksize=5000)
    
    @tsMonitor
    def ggt_monthly(pro,db):
        mysql.truncateTable('astock_price_ggt_monthly',db)
        engine=mysql.getDBEngine(db)
        data = pro.ggt_monthly()
        data.to_sql('astock_price_ggt_monthly', engine, index=False, if_exists='append', chunksize=5000)
    
    @tsMonitor
    def ccass_hold_detail(pro,db):
        pass #积分不够
        #tsStockPrice.getPrice(pro,'ccass_hold_detail','astock_price_ccass_hold_detail',db)
    
 