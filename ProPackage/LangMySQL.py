# -*- coding: utf-8 -*-
"""
Created on Tue Oct 23 09:35:51 2018

@author: zhoun
"""
import pandas as pd
from ProPackage.ProConfig import *
import re
import os



def MySql_CreateTable_Companylist(mySqlDB):
    """
    创建“最终计算用的包含价格的期货持仓表” "测试用"
    :param mySqlDB:localhost
    """
    try:
        mySqlDB.Sql("""
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
        """)

    except Exception as e:
        raise e


def MySql_CreateTable_EarningTable(mySqlDB):
    """
    创建“最终计算用的包含价格的期货持仓表” "测试用"
    :param mySqlDB:localhost
    """
    try:
        mySqlDB.Sql("""
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
        """)

    except Exception as e:
        raise e


def MySql_CreateTable_SeveralEarning(mySqlDB):
    """
    创建“某一间隔天数的多头占比值对应的不同期货公司不同观察天数的盈亏情况” "测试用"
    :param mySqlDB: localhost
    :param interval:计算多头占比值的间隔天数
    """
    try:
        mySqlDB.Sql("""
        CREATE TABLE `severalearning` (
        `间隔天数` int(2) DEFAULT NULL,
        `持有天数` int(2) DEFAULT NULL,
        `会员简称` varchar(8) DEFAULT NULL,
        `合约代码` varchar(8) DEFAULT NULL,
        `多头占比` float DEFAULT NULL,
        `日期` date DEFAULT NULL,
        `开盘价` float DEFAULT NULL,
        `最高价` float DEFAULT NULL,
        `最低价` float DEFAULT NULL,
        `收盘价` float DEFAULT NULL,
        `持买仓量` int(12) DEFAULT NULL,
        `持买增减量` int(12) DEFAULT NULL,
        `持买增减量sign`  int(2) DEFAULT NULL,
        `合约持仓量` int(12) DEFAULT NULL,
        `合约持仓变化量` int(12) DEFAULT NULL,
        `交易盈亏` float DEFAULT NULL,
        `累计持仓盈亏` float DEFAULT NULL,
        `总盈亏` float DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci 
        """)

    except Exception as e:
        raise e

def MySql_CreateTable_RankEarning(mySqlDB):
    """
    创建“最终计算用的包含价格的期货持仓表” "测试用"
    :param mySqlDB:localhost
    """
    try:
        mySqlDB.Sql("""
        CREATE TABLE `rankearning` (
          `期货品种` varchar(6) DEFAULT NULL,
          `日期` date DEFAULT NULL,
          `开盘价` float DEFAULT NULL,
          `最高价` float DEFAULT NULL,
          `最低价` float DEFAULT NULL,
          `收盘价` float DEFAULT NULL,
          `合约持仓量` float DEFAULT NULL,
          `limit1` float DEFAULT NULL,
          `limit2` float DEFAULT NULL,
          `limit3` float DEFAULT NULL,
          `limit4` float DEFAULT NULL,
          `limit5` float DEFAULT NULL,
          `limit6` float DEFAULT NULL,
          `limit7` float DEFAULT NULL,
          `limit8` float DEFAULT NULL,
          `limit9` float DEFAULT NULL,
          `limit10` float DEFAULT NULL,
          `limit11`  float DEFAULT NULL,
          `limit12` float DEFAULT NULL,
          `limit13` float DEFAULT NULL,
          `limit14` float DEFAULT NULL,
          `limit15` float DEFAULT NULL,
          `limit16` float DEFAULT NULL,
          `limit17` float DEFAULT NULL,
          `limit18` float DEFAULT NULL,
          `limit19` float DEFAULT NULL,
          `limit20` float DEFAULT NULL,
          KEY `idx_rankearning_日期` (`日期`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
        """)

    except Exception as e:
        raise e


def MySql_CreateTable_RankEarningAnswer(mySqlDB):
    """
    创建“最终计算用的包含价格的期货持仓表” "测试用"
    :param mySqlDB:localhost
    """
    try:
        mySqlDB.Sql("""
        CREATE TABLE `rankearninganswer` (
          `区间左值` varchar(8) DEFAULT NULL,
          `总盈亏` float DEFAULT NULL,
          `持有天数` int(1) DEFAULT NULL,
          `交易次数` int(1) DEFAULT NULL,
          `前多少排名` int(1) DEFAULT NULL,
          `观察天数`int(1) DEFAULT NULL,
          `训练开始日期` date DEFAULT NULL,
          `训练结束日期` date DEFAULT NULL,
          `测试开始日期` date DEFAULT NULL,
          `测试结束日期` date DEFAULT NULL
          # KEY `idx_rankearning_日期` (`日期`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
        """)

    except Exception as e:
        raise e


def MySql_BatchInsertRankEarningAnswer(mySqlDB, values, proLog=None, isLog=False):
    try:
        if len(values) > 0:
            mySqlDB.Sqls("""
             INSERT INTO `rankearninganswer`(`区间左值`,`总盈亏`,`持有天数`,`交易次数`,`前多少排名`,`观察天数`,
             `训练开始日期`,`训练结束日期`,`测试开始日期`,`测试结束日期`) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",values)
            if (isLog and proLog != None):
                proLog.Log('Table rankearninganswer BatchInsert %d Successfully ' % len(values))
        else:
            if (isLog and proLog != None):
                proLog.Log('Table rankearninganswer BatchInsert Nothing')
    except Exception as e:
        raise e

def MySql_BatchInsertCompanylist(mySqlDB, values, proLog=None, isLog=False):
    """
    将数据批量加入“期货公司单品种当日盈亏表”
    :param mySqlDB: localhost
    :param values: np.ndarray
    """
    try:
        if len(values) > 0:
            mySqlDB.Sqls('''
            insert into companylist(合约代码,日期,交易所,会员简称,持买仓量,持买增减量,持卖仓量,持卖增减量,
            期货品种) 
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s)''', values)
            if (isLog and proLog != None):
                proLog.Log('Table companylist BatchInsert %d Successfully ' % len(values))
        else:
            if (isLog and proLog != None):
                proLog.Log('Table companylist BatchInsert Nothing')
    except Exception as e:
        raise e


def MySql_BatchInsertEarningTable(mySqlDB, values, proLog=None, isLog=False):
    """
    将数据批量加入“期货公司单品种当日盈亏表”
    :param mySqlDB: localhost
    :param values: np.ndarray
    """
    try:
        if len(values) > 0:
            mySqlDB.Sqls('''
            insert into earningtabletest(合约代码, 日期, 交易所, 会员简称, 持买仓量, 持买增减量,
                  持卖仓量, 持卖增减量, 合约持仓量, 期货品种, 开盘价, 最高价,
                  最低价, 收盘价, 当日结算价, 净持仓, 净持仓变动, 当日盈亏) 
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', values)
            if isLog and proLog is not None:
                proLog.Log('Table earningtabletest BatchInsert %d Successfully ' % len(values))
        else:
            if isLog and proLog is not None:
                proLog.Log('Table earningtabletest BatchInsert Nothing')
    except Exception as e:
        raise e


def MySql_BatchInsertSeveralEarning(mySqlDB, values, proLog=None, isLog=False):
    """
    将数据批量加入“多间隔天数多持有天数”
    :param mySqlDB: localhost
    :param values: np.ndarray
    """
    try:
        if len(values) > 0:
            mySqlDB.Sqls('''
            insert into severalearning(间隔天数, 持有天数, 会员简称, 合约代码, 多头占比, 日期, 开盘价, 最高价,
                  最低价, 收盘价, 持买仓量, 持买增减量, 持买增减量sign, 合约持仓量, 合约持仓变化量, 
                  交易盈亏, 累计持仓盈亏,总盈亏) 
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', values)
            if isLog and proLog is not None:
                proLog.Log('Table severalearning BatchInsert %d Successfully ' % len(values))
        else:
            if isLog and proLog is not None:
                proLog.Log('Table severalearning BatchInsert Nothing')
    except Exception as e:
        raise e


def MySql_BatchInsertRankEarning(mySqlDB, values, proLog=None, isLog=False):
    """
    将数据批量加入“期货公司单品种当日盈亏表”
    :param mySqlDB: localhost
    :param values: np.ndarray
    """
    try:
        if len(values) > 0:
            mySqlDB.Sqls('''
            insert into rankearning(期货品种,日期,开盘价, 最高价,最低价, 收盘价,合约持仓量,
            limit1,limit2,limit3,limit4,limit5,limit6,limit7,limit8,limit9,limit10,
            limit11,limit12,limit13,limit14,limit15,limit16,limit17,limit18,limit19,limit20) 
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', values)
            if (isLog and proLog != None):
                proLog.Log('Table rankearning BatchInsert %d Successfully ' % len(values))
        else:
            if (isLog and proLog != None):
                proLog.Log('Table rankearning BatchInsert Nothing')
    except Exception as e:
        raise e


def MySql_BatchInsertStfuture(mySqlDB, values, proLog=None, isLog=False):
    try:
        if len(values) > 0:
            mySqlDB.Sqls("""
             INSERT INTO stfutureday(FID,FRealContract,FTradeDay,FOpen,FHigh,FLow,FClose,FVol,
             FOpenInst,FAmount,FFloatMoney,FSettle,FSeq,FOffset,FFlag) 
             values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",values)
            if (isLog and proLog != None):
                proLog.Log('Table stfutureday BatchInsert %d Successfully ' % len(values))
        else:
            if (isLog and proLog != None):
                proLog.Log('Table stfutureday BatchInsert Nothing')
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
    :param mySqlDB: localhost
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
    :param mySqlDB: localhost
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
    :param mySqlDB:localhost
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


def Mysql_GetColumnData(mySqlDB, columns, company, contract, start, end):
    """
    提取
    :param mySqlDB:
    :param columns:
    :param company:
    :param contract:
    :return:
    """
    sql = 'select 日期,%s from earningtabletest where 会员简称 = "%s" and 合约代码 = "%s" and ' \
          '日期 >= "%s" and 日期 <= "%s"' % (columns, company, contract, start, end)
    results = mySqlDB.GetResults(sql)
    if len(results) > 0:
        table = pd.DataFrame(list(results))
        table.columns = ['日期', company]
        table.index = pd.to_datetime(table['日期'])
        del table['日期']
        return table


def MySql_GetContractData_Earning(mySqlDB, contract):
    """
    从earningtabledb.earningtabletest中提取单合约详细信息
    :param mySqlDB:
    :param contract:
    :return:
    """
    sql = 'SELECT * FROM earningtabletest where  合约代码 = "%s" ' % contract
    results = mySqlDB.GetResults(sql)
    table = pd.DataFrame(list(results))
    table.columns = ['合约代码', '日期', '交易所', '会员简称', '持买仓量', '持买增减量',
                     '持卖仓量', '持卖增减量', '合约持仓量', '期货品种', '开盘价', '最高价',
                     '最低价', '收盘价', '当日结算价', '净持仓', '净持仓变动', '当日盈亏']
    return table


def MySql_GetContractAComapnyData_Earning(mySqlDB, company, contract):
    """
    从earningtabledb.earningtabletest中提取单合约详细信息
    :param mySqlDB:
    :param company:
    :param contract:
    :return:
    """
    try:
        sql = 'SELECT 合约代码, 日期, 交易所, 会员简称, 持买仓量, 持买增减量, 持卖仓量, 持卖增减量, 合约持仓量,' \
              ' 期货品种, 开盘价, 最高价, 最低价, 收盘价, 当日结算价, 净持仓, 净持仓变动, 当日盈亏 FROM earningtabletest ' \
              'where  合约代码 = "%s" and 会员简称 = "%s"' % (contract, company)
        results = mySqlDB.GetResults(sql)
        table = pd.DataFrame(list(results))
        table.columns = ['合约代码', '日期', '交易所', '会员简称', '持买仓量', '持买增减量', \
                         '持卖仓量', '持卖增减量', '合约持仓量', '期货品种', '开盘价', '最高价', '最低价', '收盘价', '当日结算价', '净持仓', '净持仓变动', '当日盈亏']
        #        table.index=pd.to_datetime(table['日期'])
        table = table[table['合约持仓量'] > 0]
        return table
    except Exception as e:
        raise e


def MySql_GetSeasonDay(mySqlDB, contractname, company, start, end, shift):
    """
    从earningtable.severalearning 中按期货公司和间隔计算时间提取一段区间内所有持有天数的盈亏数据。
    :param mySqlDB:localhost
    :param company:
    :param start:
    :param end:
    :param shift:
    :return:tablebuy
    """
    contractname = contractname.split(".")[0] + "%"
    sql = 'Select * from severalearning where 会员简称= "%s" and 日期 >= "%s" ' \
          'and 日期 <= "%s" and 合约代码 LIKE "%s" and 间隔天数 = "%d" order by 日期 asc;' \
          % (company, start, end, contractname, shift)

    results = mySqlDB.GetResults(sql)
    if len(results) > 0:
        table = pd.DataFrame(list(results))
        table.columns = ['间隔天数', '持有天数', '会员简称', '合约代码', '多头占比', '日期', '开盘价', '最高价',
                         '最低价', '收盘价', '持买仓量', '持买增减量', '持买增减量sign', '合约持仓量', '合约持仓变化量',
                         '交易盈亏', '累计持仓盈亏', '总盈亏']
        table = table.sort_values(['日期', '持有天数'])
        table.index = range(len(table))
        table['日期'] = table['日期'].apply(lambda x: pd.datetime.strftime(x, '%Y-%m-%d'))
        return table


def Mysql_GetDates(mySqlDB, startdate, enddate):
    """
    提取一段时间区间内的完整交易日期序列
    :param mySqlDB:
    :param startdate:
    :param enddate:
    :return:
    """
    sql = 'select distinct 日期 FROM earningtabledb.earningtabletest where 日期 >= "%s" and 日期 <= "%s";' % (
        startdate, enddate)
    results = mySqlDB.GetResults(sql)
    Dates = pd.DataFrame(list(results))
    Dates.columns = ['Dates']
    Dates['Dates'] = [i.strftime("%Y-%m-%d") for i in Dates['Dates']]
    Dates.index = pd.to_datetime(Dates['Dates'])
    return Dates


def Mysql_CheckDate(mySqlDB,tablename):
    """
    查询Companylist表中最新的日期
    :param mySqlDB:
    :return:
    """
    Date={'companylist':'日期','earningtabletest':'日期','rankearning':'日期',
          'texchangefutureday':'FDate','texchangefuturerank':'FDate','stfutureday':'FTradeDay'}
    sql = 'select {date} from {tablename} order by {date} desc limit 1'.format(
        tablename = tablename,date = Date[tablename])
    results = mySqlDB.GetResults(sql)
    print("{tablename}表中截至日期为：{date}".format(tablename = tablename,
                                             date = pd.datetime.strftime(results[0][0], '%Y-%m-%d')))


def Mysql_CheckSeveralearningDate(mySqlDB):
    """
    查询Severalearning距离最新日期前16日的日期，需要从该日期开始更新数据库
    :param mySqlDB:
    :return:
    """
    sql = 'select * from (select distinct 日期 from severalearning where ' \
          '日期 >="2018-10-01" order by 日期 desc limit 16) as A order by 日期 asc limit 1;'
    results = mySqlDB.GetResults(sql)
    print("companylist表中截至日期为：%s" % pd.datetime.strftime(results[0][0], '%Y-%m-%d'))


def Mysql_GetMainContracts(mySqlDB, contractname, start, end):
    """
    获得某一品种的所有主力合约名称和存续期
    :param mySqlDB:localhost
    :param contractname:
    :return:
    """
    contractname = contractname.split('.')[0] + '%'
    sql = 'SELECT distinct(FRealContract) FROM stfutureday where FFlag = 1 ' \
          'and FRealContract like "{contractname}" and FTradeDay Between "{start}" AND "{end}"' \
          ' order by FTradeDay;'.format(contractname=contractname, start=start, end=end)
    results = mySqlDB.GetResults(sql)
    # ans = pd.DataFrame(list(results))
    # ans.columns = ['合约代码', '日期']
    # ans.index = [pd.datetime.strftime(x, '%Y-%m-%d') for x in ans['日期']]
    # ans.index = pd.to_datetime(ans['日期'])
    results = pd.np.array(results).ravel()
    return results


def Mysql_GetBuyOI(mySqlDB, comany, contract):
    """
    返回期货公司在某合约上所有持买仓量
    :param mySqlDB:
    :param comany:
    :param contract:
    :return:
    """
    sql = 'SELECT `日期`,`持买仓量` FROM `companylist` WHERE `会员简称` = "{company}" ' \
          'AND `合约代码` = "{contract}"'.format(company=comany, contract=contract)
    results = mySqlDB.GetResults(sql)
    ans = pd.DataFrame(list(results))
    ans.columns = ['日期', '持买仓量']
    ans.index = pd.to_datetime(ans['日期'])
    return ans


def Mysql_GetRankTopNmaes(mySqlDB, date, contractname, limit):
    """
    返回当日多头持仓排名前limit的期货公司名称以及持仓数量合计值
    :param mySqlDB: localhost
    :param date:
    :param contract:
    :param limit:
    :return:
    """
    contractname = contractname.split('.')[0] + '9999'
    sql = 'SELECT `FParticipantABBR`,`FNumber` FROM `texchangefuturerank` ' \
          'WHERE FDate = "{date}" AND FRealContract = "{contract}" AND FType = "持买量" ' \
          'ORDER BY FRank LIMIT {limit}'.format(date=date, contract=contractname, limit=limit)
    results = mySqlDB.GetResults(sql)
    results = pd.np.array(results)
    return results


def Mysql_GetBuyOIIndex(mySqlDB, contractname, start, end):
    """
    计算某合约
    :param mySqlDB: localhost
    :param contractname:
    :param start:
    :param end:
    :return:
    """
    contractname2 = contractname.split('.')[0] + '9999'
    sql = 'SELECT `FTradeDay`,`FOpen`,`FHigh`,`FLow`,`FClose`,`FOpenInst` FROM `stfutureday` WHERE `FRealContract` = ' \
          '"{contractname}"  AND `FTradeDay` BETWEEN "{start}" AND "{end}" ORDER BY `FTradeDay`' \
          ''.format(contractname=contractname2, start=start, end=end)
    results = mySqlDB.GetResults(sql)
    ans = pd.DataFrame(list(results))
    ans.columns = ['日期', '开盘价', '最高价', '最低价', '收盘价', '持买仓量']
    ans.index = pd.to_datetime(ans['日期'])
    ans[['开盘价', '最高价', '最低价', '收盘价', '持买仓量']] = \
        ans[['开盘价', '最高价', '最低价', '收盘价', '持买仓量']].applymap(lambda x: round(x))
    ans.insert(0, '期货品种', contractname.split('.')[0])
    ans['日期'] = [i.strftime("%Y-%m-%d") for i in ans['日期']]
    return ans


def Mysql_GetRankLimit(mySqlDB,contractname,start,end,limit):
    """
    获得前limit排名的价格持仓数据
    :param mySqlDB:
    :param contractname: 'RB'
    :param limit: 'limit5'
    :return:
    """
    sql = 'SELECT `期货品种`,`日期`,`开盘价`,`最高价`,`最低价`,`收盘价`,`合约持仓量`,{limit} FROM `rankearning` ' \
          'WHERE `日期` BETWEEN "{start}" AND "{end}" ORDER BY `日期` ASC'.format(limit=limit, start=start, end=end)
    results = mySqlDB.GetResults(sql)
    ans = pd.DataFrame(list(results))
    ans.columns = ['期货品种','日期','开盘价','最高价','最低价','收盘价','合约持仓量',limit]
    ans['日期'] = [pd.datetime.strftime(x, '%Y-%m-%d') for x in ans['日期']]
    ans.index = pd.to_datetime(ans['日期'])
    return ans


def Mysql_GetRankEarningAnswer(mySqlDB):
    sql = 'SELECT * FROM `rankearninganswer` '
    results = mySqlDB.GetResults(sql)
    ans = pd.DataFrame(list(results))
    ans.columns = ["区间左值","总盈亏","持有天数","交易次数","前多少排名",
                   "观察天数","训练开始日期","训练结束日期","测试开始日期","测试结束日期"]
    return ans


def Mysql_CopyData(mySqlDB,tablename,start):
    """

    :param mySqlDB: reader
    :param tablename:
    :param start:
    :return:
    """
    Date = {'stfutureday':'FTradeDay'}
    sql = 'SELECT * FROM {tablename} WHERE {date} >= "{start}"'.format(tablename=tablename,
                                                                       date=Date[tablename],start=start)
    results = mySqlDB.GetResults(sql)
    return results


from sqlalchemy import create_engine

def getEnging(db,user,password,host,port):
    mysqlalchemy = 'mysql+pymysql://{user}:{password}@{host}:{port}/{db}?charset=' \
                   'utf8'.format(user=user,password=password,host=host,port=port,db=db)
    engine = create_engine(mysqlalchemy)
    return engine
