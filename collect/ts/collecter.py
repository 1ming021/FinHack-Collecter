import sys
sys.path.append("..")
sys.path.append("../..")
from library.config import config
from library.mysql import mysql
from collect.ts.astockbasic import tsAStockBasic
from collect.ts.astockprice import tsAStockPrice
from collect.ts.astockfinance import tsAStockFinance
from collect.ts.astockindex import tsAStockIndex
from collect.ts.astockmarket import tsAStockMarket
from library.thread import collectThread
import tushare as ts


class tsCollecter:
    def __init__(self):
        cfgTS=config.getConfig('ts')
        ts.set_token(cfgTS['token'])
        self.pro = ts.pro_api()
        self.db=cfgTS['db']
        self.engine=mysql.getDBEngine(cfgTS['db'])
        self.thread_list = []
        
        
    def getAll(self):
        self.getAStockBasic()
        self.getAStockPrice()
        self.getAStockFinance()
        self.getAStockMarket()
        self.getAStockIndex()
        
        for t in self.thread_list:
            t.setDaemon(True)
            t.start()

        for t in self.thread_list:
            t.join()
        

    def getAStockBasic(self):
        tsAStockBasic.stock_basic(self.pro,self.db)
        tsAStockBasic.trade_cal(self.pro,self.db)
        self.mTread(tsAStockBasic,'namechange')
        self.mTread(tsAStockBasic,'hs_const')
        self.mTread(tsAStockBasic,'stock_company')
        self.mTread(tsAStockBasic,'stk_managers')
        self.mTread(tsAStockBasic,'stk_rewards')
        self.mTread(tsAStockBasic,'new_share')


    def getAStockPrice(self):
        self.mTread(tsAStockPrice,'daily')
        self.mTread(tsAStockPrice,'weekly')
        self.mTread(tsAStockPrice,'monthly')
        self.mTread(tsAStockPrice,'adj_factor')
        self.mTread(tsAStockPrice,'suspend_d')
        self.mTread(tsAStockPrice,'daily_basic')
        self.mTread(tsAStockPrice,'moneyflow')
        self.mTread(tsAStockPrice,'stk_limit')
        self.mTread(tsAStockPrice,'limit_list')
        self.mTread(tsAStockPrice,'moneyflow_hsgt')
        self.mTread(tsAStockPrice,'hsgt_top10')
        self.mTread(tsAStockPrice,'ggt_top10')
        self.mTread(tsAStockPrice,'hk_hold')
        self.mTread(tsAStockPrice,'ggt_daily')
        

        # self.mTread(tsAStockPrice,'ggt_monthly')
        # self.mTread(tsAStockPrice,'ccass_hold_detail')
        
    def getAStockFinance(self):
        tsAStockFinance.disclosure_date(self.pro,self.db)
        self.mTread(tsAStockFinance,'income')
        self.mTread(tsAStockFinance,'balancesheet')
        self.mTread(tsAStockFinance,'cashflow')
        self.mTread(tsAStockFinance,'forecast')
        self.mTread(tsAStockFinance,'express')
        self.mTread(tsAStockFinance,'fina_indicator')
        self.mTread(tsAStockFinance,'fina_audit')
        self.mTread(tsAStockFinance,'fina_mainbz')
        self.mTread(tsAStockFinance,'dividend')

       
    def getAStockMarket(self):
        self.mTread(tsAStockMarket,'margin')
        self.mTread(tsAStockMarket,'margin_detail')
        self.mTread(tsAStockMarket,'top_list')
        self.mTread(tsAStockMarket,'top_inst')
        self.mTread(tsAStockMarket,'pledge_stat')
        self.mTread(tsAStockMarket,'pledge_detail')
        self.mTread(tsAStockMarket,'repurchase')
        self.mTread(tsAStockMarket,'concept')
        self.mTread(tsAStockMarket,'concept_detail')
        self.mTread(tsAStockMarket,'share_float')
        self.mTread(tsAStockMarket,'block_trade')
        self.mTread(tsAStockMarket,'stk_holdernumber')
        self.mTread(tsAStockMarket,'stk_holdertrade')
        self.mTread(tsAStockFinance,'top10_holders')
        self.mTread(tsAStockFinance,'top10_floatholders')
        pass
     
    def getAStockIndex(self):
        tsAStockIndex.index_basic(self.pro,self.db)
        self.mTread(tsAStockIndex,'index_daily')
        self.mTread(tsAStockIndex,'index_weekly')
        self.mTread(tsAStockIndex,'index_monthly')
        self.mTread(tsAStockIndex,'index_weight')
        self.mTread(tsAStockIndex,'index_dailybasic')
        self.mTread(tsAStockIndex,'index_classify')
        self.mTread(tsAStockIndex,'index_member')
        self.mTread(tsAStockIndex,'daily_info')
        self.mTread(tsAStockIndex,'sz_daily_info')
        self.mTread(tsAStockIndex,'ths_daily')
        pass

    
    def mTread(self,className,functionName):
        thread = collectThread(className,functionName,self.pro,self.db)
        self.thread_list.append(thread)


    

       



