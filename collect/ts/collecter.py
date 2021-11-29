import sys
sys.path.append("..")
sys.path.append("../..")
from library.config import config
from library.mysql import mysql
from collect.ts.astockbasic import tsAStockBasic
from collect.ts.astockprice import tsAStockPrice
from collect.ts.astockfinance import tsAStockFinance
from collect.ts.astockindex import tsAStockIndex
from library.thread import collectThread
import tushare as ts


class tsCollecter:
    def __init__(self):
        cfgTS=config.getConfig('ts')
        ts.set_token(cfgTS['token'])
        self.pro = ts.pro_api()
        self.db=cfgTS['db']
        self.engine=mysql.getDBEngine(cfgTS['db'])
        
        
    def getAll(self):
        # self.getAStockBasic()
        # self.getAStockPrice()
        self.getAStockFinance()
        

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
        #tsAStockFinance.disclosure_date(self.pro,self.db)
        self.mTread(tsAStockFinance,'income')
        # self.mTread(tsAStockFinance,'balancesheet')
        # self.mTread(tsAStockFinance,'cashflow')
        # self.mTread(tsAStockFinance,'forecast')
        # self.mTread(tsAStockFinance,'express')
        # self.mTread(tsAStockFinance,'fina_indicator')
        # self.mTread(tsAStockFinance,'fina_audit')
        # self.mTread(tsAStockFinance,'fina_mainbz')
        # self.mTread(tsAStockFinance,'dividend')

    
    def mTread(self,className,functionName):
        thread = collectThread(className,functionName,self.pro,self.db)
        thread.start()

    

       



