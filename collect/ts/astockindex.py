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

class tsAStockIndex:

    @tsMonitor
    def index_basic(pro,db):
        pass
    
    @tsMonitor
    def index_daily(pro,db):
        pass
    
    @tsMonitor
    def index_weekly(pro,db):
        pass
    
    @tsMonitor
    def index_monthly(pro,db):
        pass
    
    @tsMonitor
    def index_weight(pro,db):
        pass
    
    @tsMonitor
    def index_dailybasic(pro,db):
        pass
    
    @tsMonitor
    def index_classify(pro,db):
        pass
    
    @tsMonitor
    def index_member(pro,db):
        pass
    
    @tsMonitor
    def daily_info(pro,db):
        pass
    
    @tsMonitor
    def sz_daily_info(pro,db):
        pass
    
    @tsMonitor
    def ths_daily(pro,db):
        pass
    