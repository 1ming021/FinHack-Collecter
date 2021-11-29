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
    
    @tsMonitor
    def top10_holders(pro,db):
        pass
    
    @tsMonitor
    def top10_floatholders(pro,db):
        pass
    
    @tsMonitor
    def top_list(pro,db):
        pass
    
    @tsMonitor
    def top_inst(pro,db):
        pass
    
    @tsMonitor
    def pledge_stat(pro,db):
        pass
    
    @tsMonitor
    def pledge_detail(pro,db):
        pass
    
    @tsMonitor
    def repurchase(pro,db):
        pass
    
    @tsMonitor
    def concept(pro,db):
        pass
    
    @tsMonitor
    def concept_detail(pro,db):
        pass
    
    @tsMonitor
    def share_float(pro,db):
        pass
    
    @tsMonitor
    def block_trade(pro,db):
        pass
    
    @tsMonitor
    def stk_holdernumber(pro,db):
        pass
    
    # @tsMonitor
    # def stk_surv(pro,db):
    #     pass
    
    @tsMonitor
    def stk_holdertrade(pro,db):
        pass
    
    @tsMonitor
    def broker_recommend(pro,db):
        pass
    
 