#coding:utf-8
import os
import pandas as pd
import tushare as ts

des_dir = r'/media/xudi/software/stock/tushare'

pro = ts.pro_api()

def get_all_stocks():
    des_path = os.path.join(des_dir,'stocks.csv')
    data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    data.to_csv(des_path,encoding='utf8')
    
def get_trading_day_list():
    des_path = os.path.join(des_dir,'trading_day_list.csv')
    data = pro.trade_cal(exchange='', start_date='', end_date='')
    data.to_csv(des_path,encoding='utf8')

def get_basic_infomation():
    des_path = os.path.join(des_dir,'basic_infomation.csv')
    data = pro.stock_company(exchange='', fields='ts_code,chairman,manager,secretary,reg_capital,setup_date,province')
    data.to_csv(des_path,encoding='utf8')
  
def get_adj_factor():
    base_dir = r'/media/xudi/software/stock/tushare/adj_factor'  
    dates_df = pd.read_csv('/media/xudi/software/stock/tushare/trading_day_list.csv',index_col = 0)
    open_dates_df = dates_df.loc[ (dates_df['is_open'] == 1) & (dates_df['cal_date'] > 20160101) & (dates_df['cal_date'] < 20190303)]
    for date in open_dates_df['cal_date'].values:
        print date
        df = pro.adj_factor(ts_code='', trade_date=str(date))
        des_path = os.path.join(base_dir,str(date) + '.csv')
        df.to_csv(des_path)

#----------------------------------Rate--------------------------------------------
def get_shibor():
    des_path = os.path.join(des_dir,'shibor.csv')
    df = pro.shibor(start_date='20080101', end_date='20190301')
    df.to_csv(des_path,encoding = 'utf8') 
    
#----------------------------------INDEX-------------------------------------------
def get_index_information():
    base_dir = r'/media/xudi/software/stock/tushare/index_basic'
    index_list = ['MSCI','CSI','SSE','SZSE','CICC','SW','CNI']
    for index_name in index_list:
        print index_name
        des_path = os.path.join(base_dir, index_name + '.csv')
        data = pro.index_basic(market=index_name)
        data.to_csv(des_path,encoding='utf8')

def get_index_component():
    base_dir = r'/media/xudi/software/stock/tushare/index_component'
    index_list = ['399300.SZ','000016.SH','000905.SH','399330.SZ']
    start_date,end_date = '20080101','20190301'
    for index_name in index_list:
        print index_name
        des_path = os.path.join(base_dir, index_name + '.csv')
        df = pro.index_weight(index_code=index_name, start_date=start_date, end_date=end_date)
        df.to_csv(des_path)
        
def get_index_price():
    base_dir = r'/media/xudi/software/stock/tushare/index_price'
    index_list = ['399300.SZ','000016.SH','000905.SH','399330.SZ']
    start_date,end_date = '20080101','20190301'
    for index_name in index_list:
        print index_name
        des_path = os.path.join(base_dir, index_name + '.csv')
        df = pro.index_daily(ts_code=index_name, start_date=start_date, end_date=end_date)
        df.to_csv(des_path)
        
#----------------------------------INDEX-------------------------------------------
def get_industry():
    des_path = os.path.join(des_dir,'industry_sina.csv')
    df = ts.get_industry_classified(standard='sina')
    df.to_csv(des_path,encoding='utf8')
    
    des_path = os.path.join(des_dir,'industry_sw.csv')
    df = ts.get_industry_classified(standard='sw')
    df.to_csv(des_path,encoding='utf8')
    
    
if __name__ == '__main__':
    get_industry()
    print 'done!'
    