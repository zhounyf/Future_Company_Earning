from ProPackage.ProMySqlDB import ProMySqlDB
from ProPackage.ProConfig import *
from ProPackage.LangMySQL import *

from ProPackage.ProTool import *
from datetime import datetime


def get_largedata():
    L = []
    for i in range(100):
        table1 = Mysql_GetContractPrice(mySqlDBLocal, '2016-01-05')
        L.append(table1)
    table = runtimeR(pd.concat, L)
    return table


def CompanyOITableList(mySqlDB, company, start, end, proLog=None, isLog=False):
    """
    获取某期货公司一段时期内所有期货合约上的持仓情况。
    :param mySqlDB:reader
    :param company:
    :param start:
    :param end:
    :param proLog:
    :param isLog:
    :return:
    """
    daylist = MySql_GetDateLists(mySqlDB, start)
    Table = []
    for date in daylist[start:end]['Date']:
        temp = Mysql_GetCompanyOITable(mySqlDB, company, date, proLog, True)
        Table.append(temp)
    try:
        ans = pd.concat(Table)
    except ValueError:
        if (isLog and proLog != None):
            proLog.Log("%s  期间未参与交易\n" % company, True)
            proLog.Close()
        pass
    else:
        return ans


def main():
    mySqlDBLocal = ProMySqlDB(mySqlDBC_EARNINGDB_Name, mySqlDBC_UserLocal,
                              mySqlDBC_Passwd, mySqlDBC_HostLocal, mySqlDBC_Port)
    mySqlDBReader = ProMySqlDB(mySqlDBC_DataOIDB_Name, mySqlDBC_User,
                               mySqlDBC_Passwd, mySqlDBC_Host, mySqlDBC_Port)
    MySql_CreateTable_SeveralEarning(mySqlDBLocal)

    # MySql_CreateTable_ComanpyList(mySqlDBLocal)
    # MySql_CreateTable_EarningTable(mySqlDBLocal)
    # start = datetime.now()
    # table = Mysql_GetContractPrice(mySqlDBReader, '2016-01-06')
    # table = Mysql_GetCompanyOITable(mySqlDBReader, '永安期货', '2010-01-06')
    # table = MySql_GetDateLists(mySqlDBReader, '2010-01-01')
    # table = CompanyOITableList(mySqlDBReader, '永安期货', '2010-01-04', '2010-02-30')
    # table.insert(0, 0, value=table.index.values)
    # table = table.rename(columns={0: '合约代码'})
    # table = table.where(pd.notnull(table), None)
    # end = datetime.now()
    #
    # print("读取时间 \n%s" % (end - start))
    # print("转换时间")
    # values = runtimeR(frameToTuple, table)
    # print("存入时间")
    # runtime(MySql_BatchInsertComanpyListTest_Test, mySqlDBLocal, values)
    # print("行数： %d" % len(table))
    #
    # return table


if __name__ == '__main__':
    ans = runtime(main)
    # ans.insert(0, 0, value=ans.index.values)
    print(ans)
