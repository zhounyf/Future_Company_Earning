from ProPackage.LangMySQL import *
from ProPackage.ProConfig import *
from ProPackage.ProTool import *
from EarningTable.EarningSimilarCompanys import Get_Contracts


def get_compare_table(mysql_db, company, contract):
    table = MySql_GetContractAComapnyData_Earning(mysql_db, company, contract)
    table['净多头'] = [i if i > 0 else 0 for i in table['净持仓']]
    table['净多头占比'] = table['净多头'] / table['合约持仓量']
    table['多头占比'] = table['持买仓量'] / table['合约持仓量']
    table['净多头占比'] = table['净多头占比'].map(lambda x: float("%.3f" % (x * 100)))
    table['多头占比'] = table['多头占比'].map(lambda x: float("%.3f" % (x * 100)))
    table['合约持仓变化量'] = table['合约持仓量'].diff()
    return table


def MakeEarningTableOneDay(table, ratio, days, ratiokind):
    """
    计算某个持买变化占比值之后若干天的盈亏情况
    :param table: tablebuy
    :param ratio: 多头占比值
    :param days:
    :param ratiokind: 多头占比
    :return: 某多头占比值对应持有天数的盈亏情况
    """

    indexes_ = table[table[ratiokind] == ratio].index.values
    muti = MultiplierTable['合约乘数'][table['期货品种'].values[0]]
    ans = []
    for index_ in indexes_:
        if index_ == table.index[-1]:
            pass
        else:
            T = table.loc[index_:(index_ + days)].copy()
            contract = T.iloc[0, 0]
            company = T.iloc[0, 3]
            buyvalue = T.iloc[0, 4]
            changValue = T.iloc[0, 5]
            changValuesign = pd.np.sign(T.iloc[0, 5])
            date = T.iloc[0, 1]
            totalOI = T.iloc[0, 8]
            totalOIchange = T.iloc[0, 21]
            openvalue = T.iloc[1, 10]
            highvalue = T.iloc[1, 11]
            lowvalue = T.iloc[1, 12]
            closevalue = T.iloc[1, 13]
            exchangeEarning = changValuesign * (T.iloc[1, 13] - T.iloc[1, 10]) * muti  # 持买增减量与第二天收盘价开盘价价差的乘积
            T.iloc[0, 13] = pd.np.nan
            holdEarning = sum((changValuesign * T['收盘价'].diff()).fillna(0)) * muti
            totalEarning = exchangeEarning + holdEarning
            L = [days, company, contract, ratio, date, openvalue, highvalue, lowvalue, closevalue, buyvalue, changValue,
                 changValuesign, totalOI, totalOIchange, exchangeEarning, holdEarning, totalEarning]
            ans.append(L)
    ansTable = pd.DataFrame(ans, columns=['持有天数', '期货公司', '合约代码', ratiokind, '日期', '开盘价', '最高价', '最低价',
                                          '收盘价', '持买仓量', '持买增减量', '持买增减量sign', '合约持仓量', '合约持仓变化量',
                                          '交易盈亏', '累计持仓盈亏', '总盈亏'])
    return ansTable


def MakeEarningTableSeveralDay(table, days, ratiokind):
    """
    计算days天内的所有持买变化盈亏情况
    :param table:
    :param days:
    :param ratiokind:
    :return:
    """
    table.index = range(len(table))
    s = table[table['持买增减量'] != 0].index[0]  # 第一个持仓变化量出来的位置
    table = table.loc[s:]
    ratios = list(table[ratiokind].drop_duplicates())
    ratios.sort()
    if 0 in ratios:
        ratios.remove(0)
    T = []
    for day in list(range(1, days)):
        for ratio in ratios:
            temp = MakeEarningTableOneDay(table, ratio, day, ratiokind)
            T.append(temp)
    ansTable = pd.concat(T)
    return ansTable


def MakeRollingRationkind(contracttable, shifts):
    """
    根据不同间隔天数计算出的多头变化占比值
    :param table: ContractTable
    :return: ContractTable
    """
    table = contracttable.copy()
    table['持买仓量shift'] = table['持买仓量'].shift(shifts)
    table['持买增减量求和'] = table['持买增减量'].rolling(shifts).sum()
    table['合约持仓量shift'] = table['合约持仓量'].shift(1)
    table['多头变化占比'] = (table['持买增减量求和'] / table['合约持仓量shift']).map(lambda x: float("%.1f" % (x * 100)))
    contracttable['多头占比'] = table['多头变化占比']
    return contracttable


def SeveralEarning(mySqlDB, companys, Name_Date, ratiokind, shift):
    for company in companys:
        for i in range(len(Name_Date)):
            try:
                ContractTable = get_compare_table(mySqlDB, company, Name_Date[i][2])
                ContractTable = MakeRollingRationkind(ContractTable, shift)
                ContractTable.index = pd.to_datetime(ContractTable['日期'])
                ContractTable = ContractTable[Name_Date[i][0]:Name_Date[i][1]]
                AA = MakeEarningTableSeveralDay(ContractTable, 16, ratiokind)
                AA.insert(0, '间隔天数', shift)
            except (ValueError, IndexError):
                print("%s,%s数据量不够" % (company, Name_Date[i][2]))
                pass
            else:
                values = frameToTuple(AA)
                MySql_BatchInsertSeveralEarning(mySqlDB, values)
                print("%s,%s 输入完成" % (company, Name_Date[i][2]))


def MakeSeveralEarning(mySqlDB, contractname, start, end, companys, ratiokind, shifts):
    """
    将所有期货公司不同间隔时间的不同持有天数的盈亏数据导入mysql.earningtabletest
    :param mySqlDB: localhost
    :return:
    """
    for shift in range(1, shifts + 1):
        Name_Date = Get_Contracts(contractname, start, end)
        SeveralEarning(mySqlDB, companys, Name_Date, ratiokind, shift)
        print('间隔天数为' + str(shift) + ' 导入完成！')

def MakeEarningDateTable(table, Dates):
    """
    用开仓前日期和持有天数推算盈利日期。
    :param table:
    :param Dates:
    :return:
    """
    DatesV = Dates.values
    tablebuyV = table[['持有天数', '日期']].values
    newdays = []
    for temp in tablebuyV:
        newindex = Dates[Dates['Dates'] == temp[1]].index + temp[0]
        try:
            newdays.append(DatesV[newindex][0][0])
        except:
            newdays.append('-1')
            pass
    table['盈利日期'] = newdays
    return table


if __name__ == '__main__':
    mySqlDBLocal = ProMySqlDB(mySqlDBC_EARNINGDB_Name, mySqlDBC_UserLocal,
                              mySqlDBC_Passwd, mySqlDBC_HostLocal, mySqlDBC_Port)

    ContractName = 'RB.SHF'
    StartDate = '2015-01-01'
    EndDate = '2018-12-18'
    CutDate = '2016-01-01'
    Dates = Mysql_GetDates(mySqlDBLocal, StartDate, CutDate)

    table = MySql_GetSeasonDay(mySqlDBLocal, '永安期货', StartDate, CutDate, 1)
    table = MakeEarningDateTable(table,Dates)
    print(table)
    mySqlDBLocal.Close()
