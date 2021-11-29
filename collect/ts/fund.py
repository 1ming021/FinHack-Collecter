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

class tsFund:

    @tsMonitor
    def fund_basic(pro,db):
        pass
    
    @tsMonitor
    def fund_company(pro,db):
        pass
    
    @tsMonitor
    def fund_manager(pro,db):
        pass
    
    @tsMonitor
    def fund_share(pro,db):
        pass
    
    @tsMonitor
    def fund_nav(pro,db):
        pass
    
    @tsMonitor
    def fund_div(pro,db):
        pass
    
    @tsMonitor
    def fund_portfolio(pro,db):
        pass
    
    @tsMonitor
    def fund_daily(pro,db):
        pass
    
    @tsMonitor
    def fund_adj(pro,db):
        pass