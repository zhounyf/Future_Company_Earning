# -*- coding: utf-8 -*-
"""
Created on Tue Oct 23 09:35:51 2018

@author: zhoun
"""
import pandas as pd
import re
import os


def MySql_CreateTable_earningTable(mySqlDB):
    """
    创建“最终计算用的包含价格的期货持仓表”
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
          `收盘价` float DEFAULT NULL,
          `当日结算价` float DEFAULT NULL,
          `净持仓` int(12) DEFAULT NULL,
          `净持仓变动` int(12) DEFAULT NULL,
          `当日盈亏` float DEFAULT NULL,
          KEY `idx_earningtabletest_合约代码` (`合约代码`),
          KEY `idx_earningtabletest_会员简称` (`会员简称`),
          KEY `idx_earningtabletest_日期` (`日期`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
        ''')

    except Exception as e:
        raise e


def MySql_BatchInsertEarningtable_earning(mySqlDB, values, proLog=None, isLog=False):
    '''
    将数据批量加入“期货公司单品种当日盈亏表”
    '''
    try:
        if (len(values) > 0):
            mySqlDB.Sqls('''
            insert into earningtabletest(合约代码,日期,交易所,会员简称,持买仓量,持买增减量,持卖仓量,持卖增减量,
            合约持仓量,期货品种,开盘价,收盘价,当日结算价,净持仓,净持仓变动,当日盈亏) 
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', values)
            if (isLog and proLog != None):
                proLog.Log('Table texchangefutureday BatchInsert %d Successfully ' % len(values))
        else:
            if (isLog and proLog != None):
                proLog.Log('Table texchangefutureday BatchInsert Nothing')
    except Exception as e:
        raise e


def Mysql_GetContractPrice_Stfuture(mySqlDB, date):
    '''
    从informationdb.texchangefutureday提取合约价格数据
    '''
    try:
        sql = 'select * FROM earningtable where  日期 = "%s" ;' % (date)
        results = mySqlDB.GetResults(sql)
        pricetable = pd.DataFrame(list(results))
        # pricetable.columns = ['合约代码', '交易所', '开盘价', '收盘价', '当日结算价', '持仓量', '持仓量变化', '成交量', '日期']
        # pricetable['期货品种'] = pricetable['合约代码'].apply(lambda x: ''.join(re.findall(r'[A-Za-z]', x)).upper())
        return pricetable
    except Exception as e:
        raise e
