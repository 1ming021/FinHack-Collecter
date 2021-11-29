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

class tsAStockFinance:
    
    def getEndDateListDiff(table,ts_code,db,report_type=0):
        table_sql="select end_date from "+table+" where ts_code='"+ts_code+"'"
        if(report_type>1):
            table_sql=table_sql+" and report_type="+report_type
        table_df=mysql.selectToDf(table_sql,db)
        disclosure_sql="select end_date from astock_finance_disclosure_date where ts_code='"+ts_code+"'  and not ISNULL(actual_date)"
        disclosure_df=mysql.selectToDf(disclosure_sql,db)
        table_list=[]
        disclosure_list=[]
        if(not table_df.empty):
            table_list=table_df['end_date'].unique().tolist()
        if(not disclosure_df.empty):
            disclosure_list=disclosure_df['end_date'].unique().tolist()
        
        diff_list = set(disclosure_list)-set(table_list)
        diff_list=list(diff_list)
        diff_list.sort()
        return diff_list
        
        
  
    def getLastDateCountDiff(table,end_date,ts_code,db,report_type=0):
        table_sql="select * from "+table+" where ts_code='"+ts_code+"' and end_date='"+end_date+"'"
        if(report_type>1):
            table_sql=table_sql+" and report_type="+report_type
        table_res=mysql.selectToDf(table_sql,db)
        table_count=len(table_res)
        disclosure_sql="select * from astock_finance_disclosure_date where ts_code='"+ts_code+"' and end_date='"+end_date+"' and not ISNULL(actual_date)"
        disclosure_res=mysql.selectToDf(disclosure_sql,db)
        disclosure_count=len(disclosure_res)
        #print(str(table_count)+","+str(disclosure_count)+","+ts_code+","+str(report_type))
        return table_count<disclosure_count

   
    
    def getFinance(pro,api,table,fileds,db,report_type=0):
        stock_list_data=tsSHelper.getAllAStock(True,pro,db)
        stock_list=stock_list_data['ts_code'].tolist()
        #stock_list=['002624.SZ']
        # print(len(stock_list))
        # exit()
  
        for ts_code in stock_list:
            print(api+","+ts_code)
            diff_list=tsAStockFinance.getEndDateListDiff(table,ts_code,db)
            #print(diff_list)
            #exit()
            
            lastdate_sql="select max(end_date) as max from "+table+" where ts_code='"+ts_code+"'"
            if(report_type>1):
                lastdate_sql=table_sql+" and report_type="+report_type
            lastdate=mysql.selectToDf(lastdate_sql,db)
            if(lastdate.empty):
                lastdate='20000321'
            else:
                lastdate=lastdate['max'].tolist()[0]
            if lastdate==None:
                lastdate='20000321'
            diff_count=tsAStockFinance.getLastDateCountDiff(table,lastdate,ts_code,db)
            if(diff_count):
                sql="delete from "+table+" where ts_code='"+ts_code+"' and end_date='"+lastdate+"'"
                if(report_type>1):
                    sql=table_sql+" and report_type="+report_type
                mysql.delete(sql,db)
                diff_list.insert(0,lastdate)
            
            #print(diff_count)
            # print(lastdate)
            #exit()
            
            df=pd.DataFrame()
            engine=mysql.getDBEngine(db)
            
            end_list=[]
            for end_date in diff_list:
                if(lastdate>end_date):
                    continue
                end_list.append(end_date)
                
            if end_list==[]:
                continue
            f = getattr(pro, api)
            while True:
                try:
                    # print(lastdate)
                    # print(end_date)
                    # print(end_list)
                    # exit()
                    if report_type>0:
                        if len(end_list)>1:
                            df=f(ts_code=ts_code,start_date=end_list[0:1],end_date=datetime.datetime.now().strftime('%Y%m%d'),fileds=fileds,report_type=report_type)
                        else:
                            df=f(ts_code=ts_code,period=end_list[0:1],fileds=fileds,report_type=report_type)
                    else:
                        if len(end_list)>1:
                            df=f(ts_code=ts_code,start_date=end_list[0:1],end_date=datetime.datetime.now().strftime('%Y%m%d'),fileds=fileds)
                        else:
                            df=f(ts_code=ts_code,period=end_list[0:1],fileds=fileds)
                    df.to_sql(table, engine, index=False, if_exists='append', chunksize=5000)
                    break
                except Exception as e:
                    if "最多访问" in str(e):
                        print(api+":触发限流，等待重试。\n"+str(e))
                        time.sleep(15)
                        continue
                    elif "未知错误" in str(e):
                        info = traceback.format_exc()
                        print(ts_code)
                        print(period)
                        print(fileds)
                        print(report_type)
                        alert.send(api,'位置错误',str(info))
                        print(info)
                        break
                    else:
                        info = traceback.format_exc()
                        alert.send(api,'函数未知',str(info))
                        print(info)
                        break
     
    
    @tsMonitor
    def disclosure_date(pro,db):
        end_date_list=['0331','0630','0930','1231']
        engine=mysql.getDBEngine(db)
        lastdate=tsSHelper.getLastDateAndDelete(table='astock_finance_disclosure_date',filed='end_date',ts_code="",db=db)

        
        start_year=int(lastdate[0:4])
        start_mounth=int(lastdate[4:6])
        end_year=time.strftime("%Y", time.localtime())
        end_mounth=time.strftime("%m", time.localtime())
        
        end_list=[]
        for year in range(int(start_year),int(end_year)+1):
            for date in end_date_list:
                if(year==int(start_year)):
                    #首年表中最后公告日期比List日期大，则调过
                    if(start_mounth>int(date[0:2])):
                        continue;
                if(year==int(end_year)):
                    #还没到这个月份呢，跳过，先不获取
                    if(int(date[0:2])>int(end_mounth)):
                        continue
                end_list.append(str(year)+date)
        df=None
        for end_date in end_list:
            for i in range(0,100):
                while True:
                    try:
                        df = pro.disclosure_date(end_date=end_date,limit=1000,offset=1000*i)
                        df.to_sql('astock_finance_disclosure_date', engine, index=False, if_exists='append', chunksize=5000)
                        break
                    except Exception as e:
                        if "最多访问" in str(e):
                            print("disclosure_date:触发限流，等待重试。\n"+str(e))
                            time.sleep(15)
                            continue
                        else:
                            info = traceback.format_exc()
                            alert.send(api,'函数异常',str(info))
                            print(info)
                if df.empty:
                    break
        
    
    @tsMonitor
    def income(pro,db):
        fileds=""
        for i in range(1,13):
            tsAStockFinance.getFinance(pro,'income','astock_finance_income',fileds,db,i)
    
    @tsMonitor
    def balancesheet(pro,db):
        fileds=""
        for i in range(1,13):
            tsAStockFinance.getFinance(pro,'balancesheet','astock_finance_balancesheet',fileds,db,i)
    
    @tsMonitor
    def cashflow(pro,db):
        fileds=""
        for i in range(1,13):
            tsAStockFinance.getFinance(pro,'cashflow','astock_finance_cashflow',fileds,db,i)
    
    @tsMonitor
    def forecast(pro,db):
        fileds=""
        tsAStockFinance.getFinance(pro,'forecast','astock_finance_forecast',fileds,db)
    
    @tsMonitor
    def express(pro,db):
        fileds=""
        tsAStockFinance.getFinance(pro,'express','astock_finance_express',fileds,db)
    
    @tsMonitor
    def dividend(pro,db):
        engine=mysql.getDBEngine(db)
        mysql.truncateTable('astock_finance_dividend',db)
        stock_list_data=tsSHelper.getAllAStock(True,pro,db)
        stock_list=stock_list_data['ts_code'].tolist()
        for ts_code in stock_list:
            while True:
                try:
                    df = pro.dividend(ts_code=ts_code)
                    df.to_sql('astock_finance_dividend', engine, index=False, if_exists='append', chunksize=5000)
                    break
                except Exception as e:
                    if "最多访问" in str(e):
                        print("dividend:触发限流，等待重试。\n"+str(e))
                        time.sleep(15)
                        continue
                    else:
                        info = traceback.format_exc()
                        alert.send('dividend','函数异常',str(info))
                        print(info)
    
    @tsMonitor
    def fina_indicator(pro,db):
        fileds="ts_code,ann_date,end_date,eps,dt_eps,total_revenue_ps,revenue_ps,capital_rese_ps,surplus_rese_ps,undist_profit_ps,extra_item,profit_dedt,gross_margin,current_ratio,quick_ratio,cash_ratio,invturn_days,arturn_days,inv_turn,ar_turn,ca_turn,fa_turn,assets_turn,op_income,valuechange_income,interst_income,daa,ebit,ebitda,fcff,fcfe,current_exint,noncurrent_exint,interestdebt,netdebt,tangible_asset,working_capital,networking_capital,invest_capital,retained_earnings,diluted2_eps,bps,ocfps,retainedps,cfps,ebit_ps,fcff_ps,fcfe_ps,netprofit_margin,grossprofit_margin,cogs_of_sales,expense_of_sales,profit_to_gr,saleexp_to_gr,adminexp_of_gr,finaexp_of_gr,impai_ttm,gc_of_gr,op_of_gr,ebit_of_gr,roe,roe_waa,roe_dt,roa,npta,roic,roe_yearly,roa2_yearly,roe_avg,opincome_of_ebt,investincome_of_ebt,n_op_profit_of_ebt,tax_to_ebt,dtprofit_to_profit,salescash_to_or,ocf_to_or,ocf_to_opincome,capitalized_to_da,debt_to_assets,assets_to_eqt,dp_assets_to_eqt,ca_to_assets,nca_to_assets,tbassets_to_totalassets,int_to_talcap,eqt_to_talcapital,currentdebt_to_debt,longdeb_to_debt,ocf_to_shortdebt,debt_to_eqt,eqt_to_debt,eqt_to_interestdebt,tangibleasset_to_debt,tangasset_to_intdebt,tangibleasset_to_netdebt,ocf_to_debt,ocf_to_interestdebt,ocf_to_netdebt,ebit_to_interest,longdebt_to_workingcapital,ebitda_to_debt,turn_days,roa_yearly,roa_dp,fixed_assets,profit_prefin_exp,non_op_profit,op_to_ebt,nop_to_ebt,ocf_to_profit,cash_to_liqdebt,cash_to_liqdebt_withinterest,op_to_liqdebt,op_to_debt,roic_yearly,total_fa_trun,profit_to_op,q_opincome,q_investincome,q_dtprofit,q_eps,q_netprofit_margin,q_gsprofit_margin,q_exp_to_sales,q_profit_to_gr,q_saleexp_to_gr,q_adminexp_to_gr,q_finaexp_to_gr,q_impair_to_gr_ttm,q_gc_to_gr,q_op_to_gr,q_roe,q_dt_roe,q_npta,q_opincome_to_ebt,q_investincome_to_ebt,q_dtprofit_to_profit,q_salescash_to_or,q_ocf_to_sales,q_ocf_to_or,basic_eps_yoy,dt_eps_yoy,cfps_yoy,op_yoy,ebt_yoy,netprofit_yoy,dt_netprofit_yoy,ocf_yoy,roe_yoy,bps_yoy,assets_yoy,eqt_yoy,tr_yoy,or_yoy,q_gr_yoy,q_gr_qoq,q_sales_yoy,q_sales_qoq,q_op_yoy,q_op_qoq,q_profit_yoy,q_profit_qoq,q_netprofit_yoy,q_netprofit_qoq,equity_yoy,rd_exp,update_flag"
        tsAStockFinance.getFinance(pro,'fina_indicator','astock_finance_indicator',fileds,db)
    
    @tsMonitor
    def fina_audit(pro,db):
        fileds=""
        tsAStockFinance.getFinance(pro,'fina_audit','astock_finance_audit',fileds,db)
    
    @tsMonitor
    def fina_mainbz(pro,db):
        fileds=""
        tsAStockFinance.getFinance(pro,'fina_mainbz','astock_finance_mainbz',fileds,db)

