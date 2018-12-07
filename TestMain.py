from ProPackage.ProMySqlDB import ProMySqlDB
from ProPackage.ProConfig import *
from ProPackage.LangMySQL import MySql_CreateTable_earningTable, MySql_BatchInsertEarningtable_earning, \
    Mysql_GetContractPrice_Stfuture

from ProPackage.ProTool import *
from sqlalchemy import create_engine
import pymysql
from datetime import datetime


def main():
    mySqlDBLocal = ProMySqlDB(mySqlDBC_EARNINGDB_Name, mySqlDBC_UserLocal,
                              mySqlDBC_Passwd, mySqlDBC_HostLocal, mySqlDBC_Port)
    mySqlDBReader = ProMySqlDB(mySqlDBC_DataOIDB_Name, mySqlDBC_User,
                               mySqlDBC_Passwd, mySqlDBC_Host, mySqlDBC_Port)

    # MySql_CreateTable_earningTable(mySqlDBLocal)
    # print("读取时间")
    start = datetime.now()
    L = []
    for i in range(100):
        table1 = Mysql_GetContractPrice_Stfuture(mySqlDBLocal, '2016-01-05')
        L.append(table1)
    table = runtimeR(pd.concat, L)
    end = datetime.now()
    print("读取时间 \n%s" %(end - start))
    print("转换时间")
    values = runtimeR(frameToTuple, table)
    print("存入时间")
    runtime(MySql_BatchInsertEarningtable_earning, mySqlDBLocal, values)
    print("行数： %d" %len(table))

    return table


if __name__ == '__main__':
    # ans = main()
    runtime(main)

