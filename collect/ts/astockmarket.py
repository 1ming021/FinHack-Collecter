import sys
sys.path.append("..")
sys.path.append("../..")
import datetime
from library.config import config
from library.mysql import mysql
from collect.ts.helper import tsSHelper
from library.monitor import tsMonitor
import time
import pandas as pd
import traceback
from library.alert import alert

class tsAStockMarket:
    @tsMonitor
    def margin(pro,db):
        tsSHelper.getDataWithLastDate(pro,'margin','astock_market_margin',db)
    
    @tsMonitor
    def margin_detail(pro,db):
        tsSHelper.getDataWithLastDate(pro,'margin_detail','astock_market_margin_detail',db)
    
    # @tsMonitor
    # def top10_holders(pro,db):
    #     pass
    
    # @tsMonitor
    # def top10_floatholders(pro,db):
    #     pass
    
    @tsMonitor
    def top_list(pro,db):
        tsSHelper.getDataWithLastDate(pro,'top_list','astock_market_top_list',db)
    
    @tsMonitor
    def top_inst(pro,db):
        tsSHelper.getDataWithLastDate(pro,'top_inst','astock_market_top_inst',db)
    
    @tsMonitor
    def pledge_stat(pro,db):
        tsSHelper.getDataWithLastDate(pro,'pledge_stat','astock_market_pledge_stat',db,'end_date')
 
    
    @tsMonitor
    def pledge_detail(pro,db):
        tsSHelper.getDataWithCodeAndClear(pro,'pledge_detail','astock_market_pledge_detail',db)

    
    @tsMonitor
    def repurchase(pro,db):
        tsSHelper.getDataWithLastDate(pro,'repurchase','astock_market_repurchase',db,'ann_date')
 
    
    @tsMonitor
    def concept(pro,db):
        mysql.truncateTable('astock_market_concept',db)
        engine=mysql.getDBEngine(db)
        data = pro.concept()
        data.to_sql('astock_market_concept', engine, index=False, if_exists='append', chunksize=5000)
    
    @tsMonitor
    def concept_detail(pro,db):
        tsSHelper.getDataWithCodeAndClear(pro,'concept_detail','astock_market_concept_detail',db)
    
    @tsMonitor
    def share_float(pro,db):
        tsSHelper.getDataWithLastDate(pro,'share_float','astock_market_share_float',db,'ann_date')
    
    @tsMonitor
    def block_trade(pro,db):
        tsSHelper.getDataWithLastDate(pro,'block_trade','astock_market_block_trade',db)
    
    @tsMonitor
    def stk_holdernumber(pro,db):
        tsSHelper.getDataWithCodeAndClear(pro,'stk_holdernumber','astock_market_stk_holdernumber',db)
    
    # @tsMonitor
    # def stk_surv(pro,db):
    #     pass
    
    @tsMonitor
    def stk_holdertrade(pro,db):
        tsSHelper.getDataWithLastDate(pro,'stk_holdertrade','astock_market_stk_holdertrade',db,'ann_date')
    
    # @tsMonitor
    # def broker_recommend(pro,db):
    #     pass
    
 