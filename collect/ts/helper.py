import sys
sys.path.append("..")
sys.path.append("../..")
from library.config import config
from library.mysql import mysql
from library.alert import alert
import pandas as pd
import re
import traceback

class tsSHelper:
    def getAllAStock(fromDB=True,pro=None,db='default'):
        if fromDB:
            sql='select * from astock_basic'
            data=mysql.selectToDf(sql,db)
             
        else:
            all_stock=[]
            data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,fullname,enname,cnspell,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')
            all_stock.append(data)
            data = pro.stock_basic(exchange='', list_status='D', fields='ts_code,symbol,name,area,industry,fullname,enname,cnspell,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')
            all_stock.append(data)
            data = pro.stock_basic(exchange='', list_status='P', fields='ts_code,symbol,name,area,industry,fullname,enname,cnspell,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')
            all_stock.append(data)
            data=pd.concat(all_stock,axis=0,ignore_index=True)
        return data
        
    
    def getDataWithLastDate(pro,api,table,db,filed='trade_date'):
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
                    else:
                        info = traceback.format_exc()
                        alert.send(api,'函数异常',str(info))
                        print(info)
                        break
            #print(table+'-'+str(len(df))+'-'+day)
            res = df.to_sql(table, engine, index=False, if_exists='append', chunksize=5000)
            i=i+1        
        
    
    #查一下最后的数据是哪天
    def getLastDateAndDelete(table,filed,ts_code="",db='default'):
        db,cursor=mysql.getDB(db)
        sql = "show tables"
        cursor.execute(sql)
        tables = cursor.fetchall()
        tables_list = re.findall('(\'.*?\')',str(tables))
        tables_list = [re.sub("'",'',each)for each in tables_list]
        lastdate="20000101"
        data=[lastdate];
        sql=""
        try:
            if table in tables_list:
                if ts_code!="":
                    if filed=="":#有代码无日期字段，直接删掉代码对应记录
                        sql="delete  from "+table+" where ts_code=\""+ts_code+"\""
                        cursor.execute(sql)
                    else:#有代码有日期字段，找出最大的字段并删除
                        sql="select max("+filed+")  as res from "+table+" where ts_code=\""+ts_code+"\""
                        cursor.execute(sql)
                        data = cursor.fetchone()
                        if data['res']!=None:
                            lastdate=data['res']
                            cursor.execute("delete  from "+table+" where "+filed+"=\""+data['res']+"\" and ts_code=\""+ts_code+"\"")
                    db.commit()
                else:#没代码，找出最大字段
                    sql="select max("+filed+")  as res from "+table
                    cursor.execute(sql)
                    data = cursor.fetchone()
                if data['res']!=None:
                    sql="delete  from "+table+" where "+filed+"=\""+data['res']+"\""
                    cursor.execute(sql)
                    db.commit()
                    lastdate=data['res']
            else:
                pass
        except Exception as e:
            print("MySQL max Error:%s" % sql)
            info = traceback.format_exc()
            alert.send('GetLast','函数异常',str(info))
            print(info)
            db.close()
            return lastdate
        db.close()
        return lastdate