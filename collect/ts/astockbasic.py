import sys
from library.config import config
from library.mysql import mysql
from collect.ts.helper import tsSHelper
from library.monitor import tsMonitor
from library.alert import alert
import traceback


class tsAStockBasic:
    @tsMonitor
    def stock_basic(pro,db):
        mysql.truncateTable('astock_basic',db)
        engine=mysql.getDBEngine(db)
        data=tsSHelper.getAllAStock(False,pro,db)
        data.to_sql('astock_basic', engine, index=False, if_exists='append', chunksize=5000)

      
    @tsMonitor  
    def trade_cal(pro,db):
        mysql.truncateTable('astock_trade_cal',db)
        engine=mysql.getDBEngine(db)
        data = pro.trade_cal()
        data.to_sql('astock_trade_cal', engine, index=False, if_exists='append', chunksize=5000)

        
    @tsMonitor
    def namechange(pro,db):
        mysql.truncateTable('astock_namechange',db)
        engine=mysql.getDBEngine(db)
        data = pro.namechange()
        data.to_sql('astock_namechange', engine, index=False, if_exists='append', chunksize=5000)

    
    @tsMonitor   
    def hs_const(pro,db):
        mysql.truncateTable('astock_hs_const',db)
        engine=mysql.getDBEngine(db)
        data = pro.hs_const(hs_type='SH')
        data.to_sql('astock_hs_const', engine, index=False, if_exists='append', chunksize=5000)
        data = pro.hs_const(hs_type='SZ')
        data.to_sql('astock_hs_const', engine, index=False, if_exists='append', chunksize=5000)

       
    @tsMonitor 
    def stock_company(pro,db):
        mysql.truncateTable('astock_stock_company',db)
        engine=mysql.getDBEngine(db)
        data = pro.stock_company(exchange='SZSE', fields='ts_code,exchange,chairman,manager,secretary,reg_capital,setup_date,province,city,introduction,website,email,office,employees,main_business,business_scope')
        data.to_sql('astock_stock_company', engine, index=False, if_exists='append', chunksize=5000)
        data = pro.stock_company(exchange='SSE', fields='ts_code,exchange,chairman,manager,secretary,reg_capital,setup_date,province,city,introduction,website,email,office,employees,main_business,business_scope')
        data.to_sql('astock_stock_company', engine, index=False, if_exists='append', chunksize=5000)

    
    @tsMonitor
    def stk_managers(pro,db):
        mysql.truncateTable('astock_stk_managers',db)
        engine=mysql.getDBEngine(db)
        data = pro.stock_company()
        data.to_sql('astock_stk_managers', engine, index=False, if_exists='append', chunksize=5000)



    @tsMonitor
    def stk_rewards(pro,db):
        mysql.truncateTable('astock_stk_rewards',db)
        engine=mysql.getDBEngine(db)
        data=tsSHelper.getAllAStock(True,pro,db)
        stock_list=data['ts_code'].tolist()
        
        for i in range(0,len(stock_list),100):
            code_list=stock_list[i:i+100]
            while True:
                try:
                    df = pro.stk_rewards(ts_code=','.join(code_list))
                    df.to_sql('astock_stk_rewards', engine, index=False, if_exists='append', chunksize=5000)
                    break
                except Exception as e:
                    if "最多访问" in str(e):
                        print(self.func.__name__+":触发限流，等待重试。\n"+str(e))
                        time.sleep(15)
                        continue
                    elif "您没有访问该接口的权限" in str(e):
                        break
                    else:
                        info = traceback.format_exc()
                        alert.send('stk_rewards','函数异常',str(info))
                        print(info)
                        break
                    break
            
    
    @tsMonitor       
    def new_share(pro,db):
        mysql.truncateTable('astock_new_share',db)
        engine=mysql.getDBEngine(db)
        data = pro.new_share()
        data.to_sql('astock_new_share', engine, index=False, if_exists='append', chunksize=5000)

    