from ProPackage.LangMySQL import *
from ProPackage.ProConfig import *
from ProPackage.ProTool import *
from EarningSimilarCompanys import Get_Contracts
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

pd.set_option('mode.chained_assignment', None)


def MakeIndexContract(mysqlDB, company, contracts, dates ):
    """
    生成comany在时间区间内所有合约的持仓量汇总的情况，一般冲1月1号开始计算，
    则01合约在交割月内低持仓量忽略,用于计算相关性
    :param mysqlDB:
    :param company:
    :param contracts:
    :return:
    """
    for contract in contracts:
        Buy = Mysql_GetBuyOI(mysqlDB, '永安期货', contract)
        dates[contract] = Buy['持买仓量']
    dates[company+'OI'] = dates[contracts].sum(axis = 1)
    dates.index.name = company
    return dates


def MakeRankTable(mysqlDB, dates,contractname,limit):
    """
    计算每日排名前limit的持买仓量的合计值的时间序列
    :param mysqlDB:
    :param dates:
    :param contractname:
    :param limit:
    :return:
    """
    L = []
    for date in Dates['Dates']:
        temp = Mysql_GetRankTopNmaes(mysqlDB, date, contractname, limit)
        L.append(temp[:,1].astype(int).sum())
    dates['limit'] = L
    return L


if __name__ == '__main__':
    mySqlDBLocal = ProMySqlDB(mySqlDBC_EARNINGDB_Name, mySqlDBC_UserLocal,
                              mySqlDBC_Passwd, mySqlDBC_HostLocal, mySqlDBC_Port)

    ContractName = 'RB.SHF'
    StartDate = '2017-01-01'
    EndDate = '2017-12-31'
    CutDate = '2017-01-01'
    Dates = Mysql_GetDates(mySqlDBLocal, StartDate, EndDate)
    Dates.index = pd.to_datetime(Dates['Dates'])
    Contracts = Mysql_GetMainContracts(mySqlDBLocal, ContractName, StartDate, EndDate)

    t1 = MakeIndexContract(mySqlDBLocal, '永安期货', Contracts, Dates)
    t2 = Mysql_GetBuyOIIndex(mySqlDBLocal, ContractName, StartDate, EndDate)
    t3 = MakeRankTable(mySqlDBLocal, Dates, ContractName, 5)
    mySqlDBLocal.Close()