from ProPackage.LangMySQL import *
from ProPackage.ProConfig import *
from ProPackage.ProMySqlDB import ProMySqlDB
from ProPackage.ProTool import *
from EarningFutureRank import *
from EarningFuture import *
import os


def MakeCompanyOITableList(mySqlDB, company, start, end, proLog=None, isLog=False):
    """
    生成某期货公司一段时期内所有期货合约上的持仓情况。
    :param mySqlDB:reader
    :param company:
    :param start:
    :param end:
    :param proLog:
    :param isLog:
    :return:ContractList
    """
    daylist = MySql_GetDateLists(mySqlDB, '2018-10-01')
    Table = []
    for date in daylist[start:end]['Date']:
        temp = Mysql_GetCompanyOITable(mySqlDB, company, date, proLog, True)
        Table.append(temp)
    try:
        ans = pd.concat(Table)
        ans.insert(0, 0, value=ans.index.values)
        ans = ans.rename(columns={0: '合约代码'})
        ans = ans.where(pd.notnull(ans), None)
    except ValueError:
        if isLog and proLog is not None:
            proLog.Log("%s  期间未没有期货持仓排名\n" % company, True)
            proLog.Close()
        pass
    else:
        return ans


def MakeEarningTable(mySqlDB, mySqlDB2, contract, company, multipliertable):
    """
    从ContractList表中提取单合约单期货公司未填充null的持仓信息,之后结合texchangefutureday中价格信息，
    填充完整单合约历史持仓数据。
    :param mySqlDB:localhost
    :param mySqlDB2:reader
    :param contract:
    :param company:
    :return:earningTable,其中当日盈亏为改期货公司净持仓与当日结算价计算
    """
    table = Mysql_GetSimpleCompanyList(mySqlDB, contract, company)
    pricetable = Mysql_GetOneContractPrice(mySqlDB2, contract)
    table['日期'] = pd.to_datetime(table['日期']).map(lambda x: x.date())
    pricetable['日期'] = pd.to_datetime(pricetable['日期']).map(lambda x: x.date())
    table = table.set_index(table['日期'])
    pricetable = pricetable.set_index(pricetable['日期'])

    table = table.reindex(pricetable.index, fill_value=None)
    table[['合约代码', '日期', '交易所', '会员简称', '期货品种']] = table[['合约代码', '日期', '交易所', '会员简称', '期货品种']]. \
        apply(lambda x: x.fillna(method='ffill'))
    table['持买仓量昨日'] = (table['持买仓量'] - table['持买增减量']).shift(-1)
    table['持买仓量'] = table['持买仓量'].fillna(table['持买仓量昨日'])
    table['持买增减量昨日'] = table['持买仓量'].diff()
    table['持买增减量'] = table['持买增减量'].fillna(table['持买增减量昨日'])
    table['持卖仓量昨日'] = (table['持卖仓量'] - table['持卖增减量']).shift(-1)
    table['持卖仓量'] = table['持卖仓量'].fillna(table['持卖仓量昨日'])
    table['持卖增减量昨日'] = table['持卖仓量'].diff()
    table['持卖增减量'] = table['持卖增减量'].fillna(table['持卖增减量昨日'])

    table['当日结算价'] = pricetable['当日结算价']
    table['开盘价'] = pricetable['开盘价']
    table['最高价'] = pricetable['最高价']
    table['最低价'] = pricetable['最低价']
    table['收盘价'] = pricetable['收盘价']
    table['合约持仓量'] = pricetable['持仓量']
    table[['合约代码', '日期', '交易所', '会员简称', '期货品种']] = table[['合约代码', '日期', '交易所', '会员简称', '期货品种']]. \
        apply(lambda x: x.fillna(method='bfill'))

    table = table[table['合约代码'].notnull()]
    table = table.fillna(0)
    table['净持仓'] = table['持买仓量'] - table['持卖仓量']
    table['净持仓变动'] = table['持买增减量'] - table['持卖增减量']

    table['当日盈亏'] = ((table['持买仓量'].shift(1) * (table['当日结算价'].diff()) - \
                      table['持卖仓量'].shift(1) * (table['当日结算价'].diff())) * multipliertable['合约乘数'][
                         table['期货品种'][0]]).fillna(0)
    table['期货公司'] = company
    table['日期'] = table.index
    return table[['合约代码', '日期', '交易所', '会员简称', '持买仓量', '持买增减量',
                  '持卖仓量', '持卖增减量', '合约持仓量', '期货品种', '开盘价', '最高价',
                  '最低价', '收盘价', '当日结算价', '净持仓', '净持仓变动', '当日盈亏']]


def MakeCompanyList(mySqlDB, mySqlDB2, companys, start, end):
    """
    批量将40家期货公司的数据导入mySqlDBLocal.CompanyListTest中
    :param mySqlDB: mySqlDBLocal
    """
    for company in companys:
        ans = runtimeR(MakeCompanyOITableList, mySqlDB2, company, start, end)
        values = frameToTuple(ans)
        MySql_BatchInsertCompanylist(mySqlDB, values)
        print(company + "has done!")


def MakeEarningTables(mySqlDB, mySqlDB2, companys, multipliertable, startdate):
    """
    批量将40家期货公司以及2010年至今的所有期货合约生成的earningTable数据导入mySqlDBLocal.earningTable中
    :param mySqlDB:mySqlDBLocal
    :param mySqlDB2:mySqlDBReader
    :param multipliertable:MultiplierTable
    :param startdate:'2018-12-19'
    :return:ToMysql
    """

    contracts = runtimeR(Mysql_GetAllContractNames, mySqlDB, '2018-10-01')
    print("All of the Contracts is %d" % (len(contracts)))
    for company in companys:
        for contract in contracts:
            try:
                ans = MakeEarningTable(mySqlDB, mySqlDB2, contract[0], company, multipliertable)
                ans.index = pd.to_datetime(ans.index)
                ans2 = ans[startdate:]
                values = frameToTuple(ans2)
                MySql_BatchInsertEarningTable(mySqlDB, values)
                print("%s,%s has done!" % (company, contract[0]))
            except (ValueError, KeyError):
                print("%s,%s is wrong!" % (company, contract[0]))
                pass

def MakeRankeEarningTables(mySqlDB,contractName,start,end):
    """
    将某品种的指数持仓量，指数价格(成交量加权)，以及前20排名按各种累计排名合计的持仓量导入earningtabledb.rankearning
    :param mySqlDB:
    :param contractName:
    :param start:
    :param end:
    :return:
    """
    Dates = Mysql_GetDates(mySqlDB, start, end)
    PriceTable = Mysql_GetBuyOIIndex(mySqlDB, contractName, start, end)
    for i in range(1, 21):
        r = MakeRankTable(mySqlDB, Dates, contractName, i)
        PriceTable['limit' + str(i)] = r['limit']
    values = frameToTuple(PriceTable)
    print(values[0])
    # MySql_BatchInsertRankEarning(mySqlDBLocal, values)


def UPdateTables(tablename):
    engineLocal = getEnging(mySqlDBC_EARNINGDB_Name, mySqlDBC_UserLocal,
                              mySqlDBC_Passwd, mySqlDBC_HostLocal, mySqlDBC_Port)
    engineReader = getEnging(mySqlDBC_DataOIDB_Name, mySqlDBC_User,
                               mySqlDBC_Passwd, mySqlDBC_Host, mySqlDBC_Port)
    sql = "SELECT * FROM `informationdb`.{tablename} WHERE `FDate` > '2019-01-08'".format(tablename = tablename)
    table = pd.read_sql(sql,con=engineReader)
    table.to_sql(tablename,con=engineLocal,if_exists='append',index=False)
    Mysql_CheckDate(mySqlDBLocal,tablename)


if __name__ == '__main__':
    mySqlDBLocal = ProMySqlDB(mySqlDBC_EARNINGDB_Name, mySqlDBC_UserLocal,
                              mySqlDBC_Passwd, mySqlDBC_HostLocal, mySqlDBC_Port)
    # mySqlDBReader = ProMySqlDB(mySqlDBC_DataOIDB_Name, mySqlDBC_User,
    #                            mySqlDBC_Passwd, mySqlDBC_Host, mySqlDBC_Port)
    # mySqlDBReaderInformation = ProMySqlDB(mySqlDBC_InformationDB_Name, mySqlDBC_User,
    #                            mySqlDBC_Passwd, mySqlDBC_Host, mySqlDBC_Port)

    # Mysql_CheckDate(mySqlDBLocal, 'rankearning')
    # MakeCompanyList(mySqlDBLocal, mySqlDBReader, Companys, '2019-01-04', '2019-01-21')
    # MakeEarningTables(mySqlDBLocal, mySqlDBReader, Companys, MultiplierTable, '2019-01-04')
    MakeRankeEarningTables(mySqlDBLocal,'RB.SHFE','2019-01-02', '2019-01-21')


    # mySqlDBReaderInformation.Close()
    # mySqlDBReader.Close()
    mySqlDBLocal.Close()
#