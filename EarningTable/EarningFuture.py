from ProPackage.LangMySQL import *
from ProPackage.ProConfig import *
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
            totalOIchange = T.iloc[0, 19]
            openvalue = T.iloc[1, 10]
            closevalue = T.iloc[1, 11]
            exchangeEarning = changValuesign * (T.iloc[1, 11] - T.iloc[1, 10]) * muti  # 持买增减量与第二天收盘价开盘价价差的乘积
            T.iloc[0, 11] = pd.np.nan
            holdEarning = sum((changValuesign * T['收盘价'].diff()).fillna(0)) * muti
            totalEarning = exchangeEarning + holdEarning
            L = [days, company, contract, ratio, date, openvalue, closevalue, buyvalue, changValue,
                 changValuesign, totalOI, totalOIchange, exchangeEarning, holdEarning, totalEarning]
            ans.append(L)
    ansTable = pd.DataFrame(ans, columns=['观察天数', '期货公司', '合约代码', ratiokind, '日期', '开盘价', '收盘价',
                                          '持买仓量', '持买增减量', '持买增减量sign', '合约持仓量', '合约持仓变化量',
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


