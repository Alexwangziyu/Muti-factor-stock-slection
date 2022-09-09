#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from jqdatasdk import *
auth('id','password') ##joinquant login in


# In[ ]:


import pandas as pd
from pandas import Series, DataFrame
import numpy as np
import scipy.stats as scs
from scipy.stats import ttest_1samp


# In[ ]:


factors = ['PE', 'PB', 'PS', 'EPS', 'B/M',
           'ROE', 'ROA', 'gross_profit_margin', 'inc_net_profit_year_on_year', 'inc_net_profit_annual', 
                     'inc_operation_profit_year_on_year', 'inc_operation_profit_annual', 'GP/R', 'P/R',
           'net_profit', 'operating_revenue', 'capitalization', 'circulating_cap', 'market_cap', 'circulating_market_cap',
                     'L/A', 'FAP',
           'turnover_ratio','pcf','operating profit','basic EPS'] ##因子库


# In[ ]:


##需要大概25秒运行时间
indexset = get_index_stocks('000300.XSHG')##分类 将300只成分股分到stockclassify这个字典里 key为行业指数
nengyuan,cailiao,gongye,kexuan,richang,yiliao,jinrong,xinxi,dianxin,gongyong,dichan=[],[],[],[],[],[],[],[],[],[],[]
for x in indexset:
    induname=get_industry(x, date=None)[x]['jq_l1']['industry_name']
    if induname == '能源指数':
        nengyuan.append(x)
    elif induname == '材料指数':
        cailiao.append(x)
    elif induname == '工业指数':
        gongye.append(x)
    elif induname == '可选消费指数':
        kexuan.append(x)
    elif induname == '日常消费指数':
        richang.append(x)
    elif induname == '医疗保健指数':
        yiliao.append(x)
    elif induname == '金融指数':
        jinrong.append(x)
    elif induname == '信息技术指数':
        xinxi.append(x)
    elif induname == '电信服务指数':
        dianxin.append(x)
    elif induname == '公用事业指数':
        gongyong.append(x)
    elif induname == '房地产指数':
        dichan.append(x)
    else:
        print('行业错误')
stockclassify={'能源指数':nengyuan,'材料指数':cailiao,'工业指数':gongye,'可选消费指数':kexuan,
                  '日常消费指数':richang,'医疗保健指数':yiliao,'金融指数':jinrong,'信息技术指数':xinxi,
                   '电信服务指数':dianxin,'公用事业指数':gongyong,'房地产指数':dichan}
for i in stockclassify:
    print(i)
    print(len(stockclassify[i]))


# In[ ]:


def get_factors(fdate, factors,stock_set):##此函数用来提取因子 可以提取单个也可以提取一组
    q = query(                            ##但是因为我们需要对单个股票的时间序列求相关性
        valuation.code,                   ##我后面就写了个对于时间序列的遍历然后用该函数求出对应单支股票的数据
        balance.total_owner_equities/valuation.market_cap/100000000,
        valuation.pe_ratio,              ##这些都是聚宽里面的指标id，可以去聚宽那个教程里面搜索get_fundamentals就知道都是啥了
        valuation.pb_ratio,
        valuation.ps_ratio,
        income.basic_eps,
        indicator.roe,
        indicator.roa,
        indicator.gross_profit_margin,
        indicator.inc_net_profit_year_on_year,
        indicator.inc_net_profit_annual,
        indicator.inc_operation_profit_year_on_year,
        indicator.inc_operation_profit_annual,
        income.total_profit/income.operating_revenue,
        income.net_profit/income.operating_revenue,
        income.net_profit,
        income.operating_revenue,
        valuation.capitalization,
        valuation.circulating_cap,
        valuation.market_cap,
        valuation.circulating_market_cap,
        balance.total_liability/balance.total_assets,
        balance.fixed_assets/balance.total_assets,
        valuation.turnover_ratio,
        valuation.pcf_ratio,
        income.operating_profit,
        income.basic_eps
        ).filter(
        valuation.code.in_(stock_set),
        valuation.circulating_market_cap
    )
    fdf = get_fundamentals(q, date=fdate)##
    return fdf ##.iloc[:,-23:]


# In[ ]:


def get_finfactors(fdate, factors,stock_set):
    #q1
    q1 = query(                                              
        balance.total_owner_equities/valuation.market_cap/100000000,
        valuation.pe_ratio,             
        valuation.pb_ratio,
        valuation.ps_ratio,
        income.basic_eps,
        indicator.roe,
        indicator.roa,
        indicator.gross_profit_margin,
        indicator.inc_net_profit_year_on_year,
        indicator.inc_net_profit_annual,
        indicator.inc_operation_profit_year_on_year,
        indicator.inc_operation_profit_annual,
        income.total_profit/income.operating_revenue,
        income.net_profit/income.operating_revenue,
        income.net_profit,
        income.operating_revenue,
        valuation.capitalization,
        valuation.circulating_cap,
        valuation.market_cap,
        valuation.circulating_market_cap,
        balance.total_liability/balance.total_assets,
        balance.fixed_assets/balance.total_assets,
        valuation.turnover_ratio,
        valuation.pcf_ratio,
        income.operating_profit,
        income.basic_eps
        ).filter(
        valuation.code.in_(stock_set),
        valuation.circulating_market_cap
    )
    dff1 = get_fundamentals(q1, date=fdate)
    #q2
    q2=query(
        finance.FINANCE_INCOME_STATEMENT.interest_net_revenue,
        finance.FINANCE_INCOME_STATEMENT.interest_expense,
        finance.FINANCE_INCOME_STATEMENT.commission_net_income,
        finance.FINANCE_INCOME_STATEMENT.manage_income,
        finance.FINANCE_INCOME_STATEMENT.investment_income,
        finance.FINANCE_INCOME_STATEMENT.fair_value_variable_income,
        finance.FINANCE_INCOME_STATEMENT.exchange_income,
        finance.FINANCE_INCOME_STATEMENT.operating_profit,
        finance.FINANCE_INCOME_STATEMENT.total_profit,
        finance.FINANCE_INCOME_STATEMENT.assurance_income,
        finance.FINANCE_INCOME_STATEMENT.compensate_loss,
        finance.FINANCE_INCOME_STATEMENT.eps
    ).filter(finance.FINANCE_INCOME_STATEMENT.code==stock_set,finance.FINANCE_INCOME_STATEMENT.pub_date>=fdate,finance.FINANCE_INCOME_STATEMENT.report_type==0).limit(1)
    dff2=finance.run_query(q2)
    #q3
    q3=query(
        finance.FINANCE_CASHFLOW_STATEMENT.net_operate_cash_flow,
        finance.FINANCE_CASHFLOW_STATEMENT.operate_cash_flow,
        finance.FINANCE_CASHFLOW_STATEMENT.net_loan_and_advance_decrease,
        finance.FINANCE_CASHFLOW_STATEMENT.net_deposit_increase,
        finance.FINANCE_CASHFLOW_STATEMENT.net_borrowing_from_central_bank,
        finance.FINANCE_CASHFLOW_STATEMENT.net_deposit_in_cb_and_ib_de,
        finance.FINANCE_CASHFLOW_STATEMENT.trade_asset_increase,
        finance.FINANCE_CASHFLOW_STATEMENT.invest_loss,
        finance.FINANCE_CASHFLOW_STATEMENT.invest_cash_flow       
    ).filter(finance.FINANCE_CASHFLOW_STATEMENT.code==stock_set,finance.FINANCE_CASHFLOW_STATEMENT.pub_date>=fdate,finance.FINANCE_CASHFLOW_STATEMENT.report_type==0).limit(1)
    dff3=finance.run_query(q3)
    #q4
    q4=query(
         finance.FINANCE_BALANCE_SHEET.cash_equivalents,
         finance.FINANCE_BALANCE_SHEET.deposit_client,
         finance.FINANCE_BALANCE_SHEET.interest_receivable,
         finance.FINANCE_BALANCE_SHEET.insurance_receivables,
         finance.FINANCE_BALANCE_SHEET.loan_and_advance,
         finance.FINANCE_BALANCE_SHEET.total_assets,
         finance.FINANCE_BALANCE_SHEET.accounts_payable,
         finance.FINANCE_BALANCE_SHEET.good_will,
         finance.FINANCE_BALANCE_SHEET.total_liability_equity
    ).filter(finance.FINANCE_BALANCE_SHEET.code==stock_set,finance.FINANCE_BALANCE_SHEET.pub_date>=fdate,finance.FINANCE_BALANCE_SHEET.report_type==0).limit(1)
    dff4=finance.run_query(q4)
    dfff1=pd.concat([dff1,dff2],axis=1)
    dfff2=pd.concat([dff3,dff4],axis=1)
    return pd.concat([dfff1,dfff2],axis=1)


# In[ ]:


##计算单个股票月收益率的函数 18年底往前推3年
def getrevenue(stkindex1):
    ##stkindex1=['000001.XSHE']
    lis=[]
    close1 = get_bars(stkindex1,25, fields=['date','close'],unit='1M',end_dt='2018-12-31',include_now=True)
    close2=close1.copy(deep=True)
    close2.drop(close2.tail(1).index,inplace=True)
    for i in range(len(close2)):
        close2['close'][i]=(close1['close'][i+1]-close1['close'][i])/close1['close'][i]
    close2.index=close2['date']
   ## print(close1)
    return close2.iloc[:,-1:]
getrevenue(['000723.XSHE'])


# In[ ]:


##使用get_factors()得到单支股票在时间序列上的因子值
timeseries=['2016-12-30','2017-01-26','2017-02-28','2017-03-31','2017-04-28',
'2017-05-31','2017-06-30','2017-07-31','2017-08-31','2017-09-29','2017-10-31',
'2017-11-30','2017-12-29','2018-01-31','2018-02-28','2018-03-30','2018-04-27',
'2018-05-31','2018-06-29','2018-07-31','2018-08-31','2018-09-28','2018-10-31',
'2018-11-30']
def gettimefactor(stockname,timeseries1):
    for i in range(len(timeseries1)):
        if i != 0:
            infor=pd.concat([infor,get_factors(timeseries1[i],factors,stockname)], axis=0, join='outer')
        else:
            infor = get_factors(timeseries1[i], factors,stockname)
    infor.insert(0,'date',timeseries)#插入date index['000568.XSHE']
    infor.index=infor['date']
    return infor.iloc[:,-26:]
def gettimefactor_fin(stockname,timeseries1):
    for i in range(len(timeseries1)):
        if i != 0:
            infor=pd.concat([infor,get_finfactors(timeseries1[i],factors,stockname)], axis=0, join='outer')
        else:
            infor = get_finfactors(timeseries1[i], factors,stockname)
    infor.insert(0,'date',timeseries)#插入date index['000568.XSHE']
    infor.index=infor['date']
    return infor.iloc[:,-56:]


# In[ ]:


def get_factors_date(fdate,stock_set):
    ans=[]
    for x in stock_set:
        q=query(finance.STK_LIST.code).filter(
                finance.STK_LIST.code==x,
                 finance.STK_LIST.start_date<=fdate
                ).limit(1)
        df=finance.run_query(q)
        if bool(1-df.empty):
             ans.append(x)
    return ans


# In[ ]:


#这一部分是主程序，先用日常消费指数股票测试一下，最后factorcor就是股价和不同因子的相关系数序列，我们在下个代码块进行T检验
list1=[]  #factorcorpe factorcoranon1就是pe/anon1和不同因子的相关系数序列
listpe=[]
listanon=[]

#stockclassify['日常消费指数'].remove('603156.XSHG')## 这个股票在2018年1月才上市 有bug 
for x in stockclassify['金融指数']:
    print(x)
    fd2=getrevenue([x])
    timeseries=fd2.index.values.tolist()
    fd1=gettimefactor_fin([x],timeseries)
    fd2.index=fd1.index
    fdhebing=pd.concat([fd2, fd1], axis=1)
    fdhebingcor=fdhebing.fillna(value=np.nan).corr()
    list1.append(fdhebingcor['close'])
    listpe.append(fdhebingcor['pe_ratio'])
    listanon.append(fdhebingcor['anon_1'])


# In[ ]:


factorcor=pd.concat(list1,axis=1)
factorcorpe=pd.concat(listpe,axis=1)
factorcoranon1=pd.concat(listanon,axis=1)
factorcor= pd.DataFrame(factorcor.values.T, index=factorcor.columns, columns=factorcor.index)
factorcorpe= pd.DataFrame(factorcorpe.values.T, index=factorcorpe.columns, columns=factorcorpe.index)
factorcoranon1= pd.DataFrame(factorcoranon1.values.T, index=factorcoranon1.columns, columns=factorcoranon1.index)


# In[ ]:


#t-test，看看那些因子强相关
factorlist=factorcor.columns.values.tolist()
factorlist.remove('operating_profit')
factorlist.remove('operating_profit')
print(factorlist)
usefulfactor={}
pecor={}
anoncor={}
for x in factorlist:
    vin=pd.isnull(factorcor[x])
    notnull=factorcor[x][~vin]
    x1,x2=ttest_1samp(notnull,0)
    x3,x4=ttest_1samp(factorcorpe[x],0)
    x5,x6=ttest_1samp(factorcoranon1[x],0)
    print(x)
    print(x1)
    print(x2)
    if x2 < 0.05:           #p value小于0.05，我们认为因子与股价变化显著相关
        usefulfactor[x]=x1
    if x4 <0.025:          #PE值跟其他因子的相关度，小于0.025我认为显著
        pecor[x]=x3
    if x6 <0.025:         #anon1值跟其他因子的相关度，小于0.025我认为显著
        anoncor[x]=x5
print(usefulfactor)
print(pecor)
print(anoncor)


# In[ ]:




