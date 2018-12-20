# -*- coding: utf-8 -*-
"""
Created on Tue Oct 23 09:35:51 2018

@author: zhoun
"""
import pandas as pd
import re
import os


def MySql_CreateTable_Companylist(mySqlDB):
    """
    创建“最终计算用的包含价格的期货持仓表” "测试用"
    :param mySqlDB:localhost
    :return:
    """
    try:
        mySqlDB.Sql('''
        CREATE TABLE `companylist` (
          `合约代码` varchar(8) DEFAULT NULL,
          `日期` date DEFAULT NULL,
          `交易所` varchar(8) DEFAULT NULL,
          `会员简称` varchar(8) DEFAULT NULL,
          `持买仓量` int(12) DEFAULT NULL,
          `持买增减量` int(12) DEFAULT NULL,
          `持卖仓量` int(12) DEFAULT NULL,
          `持卖增减量` int(12) DEFAULT NULL,
          `期货品种` varchar(6) DEFAULT NULL
          # `开盘价` float DEFAULT NULL,
          # `最高价` float DEFAULT NULL,
          # `最低价` float DEFAULT NULL,
          # `收盘价` float DEFAULT NULL,
          # `当日结算价` float DEFAULT NULL
          # KEY `idx_earningtabletest_合约代码` (`合约代码`),
          # KEY `idx_earningtabletest_会员简称` (`会员简称`),
          # KEY `idx_earningtabletest_日期` (`日期`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
        ''')

    except Exception as e:
        raise e


def MySql_CreateTable_EarningTable(mySqlDB):
    """
    创建“最终计算用的包含价格的期货持仓表” "测试用"
    :param mySqlDB:localhost
    :return:
    """
    try:
        mySqlDB.Sql('''
        CREATE TABLE `earningtabletest` (
          `合约代码` varchar(8) DEFAULT NULL,
          `日期` date DEFAULT NULL,
          `交易所` varchar(8) DEFAULT NULL,
          `会员简称` varchar(8) DEFAULT NULL,
          `持买仓量` int(12) DEFAULT NULL,
          `持买增减量` int(12) DEFAULT NULL,
          `持卖仓量` int(12) DEFAULT NULL,
          `持卖增减量` int(12) DEFAULT NULL,
          `合约持仓量` int(12) DEFAULT NULL,
          `期货品种` varchar(6) DEFAULT NULL,
          `开盘价` float DEFAULT NULL,
          `最高价` float DEFAULT NULL,
          `最低价` float DEFAULT NULL,
          `收盘价` float DEFAULT NULL,
          `当日结算价` float DEFAULT NULL,
          `净持仓` int(12) DEFAULT NULL,
          `净持仓变动` int(12) DEFAULT NULL,
          `当日盈亏` float DEFAULT NULL
          # KEY `idx_earningtabletest_合约代码` (`合约代码`),
          # KEY `idx_earningtabletest_会员简称` (`会员简称`),
          # KEY `idx_earningtabletest_日期` (`日期`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
        ''')

    except Exception as e:
        raise e


def MySql_BatchInsertCompanylist(mySqlDB, values, proLog=None, isLog=False):
    '''
    将数据批量加入“期货公司单品种当日盈亏表”
    '''
    try:
        if len(values) > 0:
            mySqlDB.Sqls('''
            insert into companylist(合约代码,日期,交易所,会员简称,持买仓量,持买增减量,持卖仓量,持卖增减量,
            期货品种) 
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s)''', values)
            if (isLog and proLog != None):
                proLog.Log('Table texchangefutureday BatchInsert %d Successfully ' % len(values))
        else:
            if (isLog and proLog != None):
                proLog.Log('Table texchangefutureday BatchInsert Nothing')
    except Exception as e:
        raise e


def MySql_BatchInsertEarningTable(mySqlDB, values, proLog=None, isLog=False):
    '''
    将数据批量加入“期货公司单品种当日盈亏表”
    '''
    try:
        if len(values) > 0:
            mySqlDB.Sqls('''
            insert into earningtabletest(合约代码, 日期, 交易所, 会员简称, 持买仓量, 持买增减量,
                  持卖仓量, 持卖增减量, 合约持仓量, 期货品种, 开盘价, 最高价,
                  最低价, 收盘价, 当日结算价, 净持仓, 净持仓变动, 当日盈亏) 
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', values)
            if isLog and proLog is not None:
                proLog.Log('Table texchangefutureday BatchInsert %d Successfully ' % len(values))
        else:
            if isLog and proLog is not None:
                proLog.Log('Table texchangefutureday BatchInsert Nothing')
    except Exception as e:
        raise e


def Mysql_GetContractPrice(mySqlDB, date):
    """
    从informationdb.texchangefutureday提取合约价格数据
    """
    try:
        sql = 'SELECT FRealContract,FExchange,FOpen,FHigh,FLow,FClose,FSettle,FOpenInst,FOpenInstChange,FVol ,FDate\
              FROM informationdb.texchangefutureday where  FDate = ' + '"' + date + '"' + ';'
        results = mySqlDB.GetResults(sql)
        pricetable = pd.DataFrame(list(results))
        pricetable.columns = ['合约代码', '交易所', '开盘价', '最高价', '最低价', '收盘价', '当日结算价', '持仓量', '持仓量变化', '成交量', '日期']
        pricetable['期货品种'] = pricetable['合约代码'].apply(lambda x: ''.join(re.findall(r'[A-Za-z]', x)).upper())
        return pricetable
    except Exception as e:
        raise e


def MySql_GetDateLists(mySqlDB, start):
    """
    获取从start日开始的日期列表
    :param mySqlDB: reader
    :param start:
    :return:
    """
    sql = 'SELECT distinct FDate FROM informationdb.texchangefuturerank where FDate >= "%s"' % start
    results = mySqlDB.GetResults(sql)
    table = pd.DataFrame(list(results))
    table.columns = ['Date']
    table = table.set_index(pd.to_datetime(table['Date']))
    table['Date'] = [i.strftime('%Y%m%d') for i in table.index]
    return table


def Mysql_GetCompanyOITable(mySqlDB, company, date, proLog=None, isLog=False):
    """
    从informationdb.texchangefuturerank数据库中提取某期货公司席位在所有合约上的多空持仓情况，并添加各合约当日价格
    :param mySqlDB: reader
    :param company:
    :param date:
    :param proLog:
    :param isLog:
    :return:
    """
    try:
        sqlbuy = '''SELECT  FDate,FExchange,FRealContract,FParticipantABBR,FNumber,FChange FROM  informationdb.texchangefuturerank 
                    where FDate = "%s" and FParticipantABBR = "%s" and FType= "持买量" and FRealContract not like "%s" and 
                    FRealContract not like "%s"''' % (date, company, '%9999', '%tv%')

        sqlsell = '''SELECT  FDate,FExchange,FRealContract,FParticipantABBR,FNumber,FChange FROM  informationdb.texchangefuturerank 
                    where FDate = "%s" and FParticipantABBR = "%s" and FType= "持卖量" and FRealContract not like "%s" and 
                    FRealContract not like "%s"''' % (date, company, '%9999', '%tv%')

        results = mySqlDB.GetResults(sqlbuy)
        tablebuy = pd.DataFrame(list(results))
        results = mySqlDB.GetResults(sqlsell)
        tablesell = pd.DataFrame(list(results))
        tablebuy.columns = ['日期', '交易所', '合约代码', '会员简称', '持买仓量', '持买增减量']
        tablesell.columns = ['日期', '交易所', '合约代码', '会员简称', '持卖仓量', '持卖增减量']
        table = pd.merge(tablebuy, tablesell, how='outer', on=['日期', '交易所', '合约代码', '会员简称'])
        table['期货品种'] = table['合约代码'].apply(lambda x: ''.join(re.findall(r'[A-Za-z]', x)).upper())
        table.set_index('合约代码', inplace=True)
    except ValueError:
        if isLog and proLog is not None:
            proLog.Log("%s %s <get_company> is valueError\n" % (company, date))
    else:
        return table


def Mysql_GetOneContractPrice(mySqlDB, contract):
    """
    获取单一合约的所有价格信息
    :param mySqlDB:reader
    :param contract:
    :return:
    """
    sql = 'SELECT FRealContract,FOpen,FHigh,FLow,FClose,FSettle,FOpenInst,' \
          'FDate FROM informationdb.texchangefutureday where  ' \
          'FRealContract =' + '"' + contract + '"' + 'order by FDate '
    results = mySqlDB.GetResults(sql)
    pricetable = pd.DataFrame(list(results))
    pricetable.columns = ['合约代码', '开盘价', '最高价', '最低价', '收盘价', '当日结算价', '持仓量', '日期']
    return pricetable


def Mysql_GetSimpleCompanyList(mySqlDB, contract, company):
    """
    获取companylist表中单合约单期货公司的原始持仓信息
    :param mySqlDB:localhost
    :param contract:
    :param company:
    :return:
    """
    sql = 'select * from companylist where 合约代码 = "%s" and 会员简称 = "%s"' % (contract, company)
    results = mySqlDB.GetResults(sql)
    table = pd.DataFrame(list(results))
    table.columns = ['合约代码', '日期', '交易所', '会员简称', '持买仓量', '持买增减量', '持卖仓量', '持卖增减量', '期货品种']
    return table


def Mysql_GetAllContractNames(mySqlDB, start):
    """
    从companylist中获得所有合约的名称
    :param mySqlDB:
    :param start:
    :return:
    """
    sql = 'select distinct 合约代码 from companylist where 日期 >= "%s"' % start
    results = mySqlDB.GetResults(sql)
    return results
