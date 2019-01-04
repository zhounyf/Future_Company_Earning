from ProPackage.LangMySQL import *
from ProPackage.ProConfig import *
from ProPackage.ProTool import *
from EarningSimilarCompanys import Get_Contracts
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
pd.set_option('mode.chained_assignment', None)


def get_compare_table(mysqldb, company, contract):
    table = MySql_GetContractAComapnyData_Earning(mysqldb, company, contract)
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
    ansTable = pd.DataFrame(ans, columns=['持有天数', '会员简称', '合约代码', ratiokind, '日期', '开盘价', '最高价', '最低价',
                                          '收盘价', '持买仓量', '持买增减量', '持买增减量sign', '合约持仓量', '合约持仓变化量',
                                          '交易盈亏', '累计持仓盈亏', '总盈亏'])
    return ansTable


def MakeEarningTableSeveralDay(table, days, ratiokind):
    """
    计算days天内的所有持买变化盈亏情况，并去掉占比值近似为0的数据
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
                AA2 = AA.set_index(pd.to_datetime(AA['日期']))
                ContractTable2 = ContractTable.set_index(pd.to_datetime(ContractTable['日期']))
                AA2[['开盘价', '最高价', '最低价', '收盘价']] = ContractTable2[['开盘价', '最高价', '最低价', '收盘价']]
                AA2.insert(0, '间隔天数', shift)
            except (ValueError, IndexError):
                print("%s,%s数据量不够" % (company, Name_Date[i][2]))
                pass
            else:
                if AA['持买仓量'].sum() > 0:
                    values = frameToTuple(AA2)
                    MySql_BatchInsertSeveralEarning(mySqlDB, values)
                    print("%s, %s, %s 输入完成" % (company, Name_Date[i][2], shift))
                # return values


def MakeSeveralEarning(mySqlDB, mySqlDB2,contract, start, end, companys, ratiokind, shifts):
    """
    将所有期货公司不同间隔时间的不同持有天数的盈亏数据导入mysql.earningtabletest
    :param mySqlDB: localhost
    :return:
    """
    Name_Date = Get_Contracts(mySqlDB2, contract, start, end)
    for shift in range(1, shifts + 1):
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


def MakeEarning(table, ratiokind='多头占比'):
    """
    返回以区间为index,间隔为0.2,15天“平均”收益情况的表，并统计相应区间的交易次数
    :param table: MySql_GetSeasonDay
    :param ratiokind:
    :return:
    """
    table = table[table['持买增减量sign'] > 0]
    L = []
    contract = table['合约代码'].values[0]
    company = table['会员简称'].values[0]
    value = table[ratiokind].drop_duplicates().values
    value.sort()
    interval = Make_Interval_Pecentage(value[0], value[-1], 0.2)
    table['interval'] = pd.cut(list(table[ratiokind]), interval, right=False)

    for day in range(1, 16):
        A = table[table['持有天数'] == day]
        A['interval'] = pd.cut(list(A[ratiokind]), interval, right=False)
        l = A.groupby(by='interval').mean().dropna()['总盈亏']
        l = l.rename(columns={'总盈亏': day})
        L.append(l)
    ans = pd.concat(L, axis=1).astype('int')
    ans['left'] = [i.left for i in ans.index]
    ans['right'] = [i.right for i in ans.index]
    ans['品种'] = ''.join(re.findall(r'[A-Za-z]', contract)).upper()
    ans['会员简称'] = company
    ans.columns = ['day1', 'day2', 'day3', 'day4', 'day5', 'day6', 'day7', 'day8', 'day9', 'day10',
                   'day11', 'day12', 'day13', 'day14', 'day15', 'interleft', 'interright', 'contract', 'company']
    counts = pd.DataFrame(table['interval'].value_counts())
    counts.columns = ['counts']
    counts = counts[counts['counts'] != 0]
    ans = pd.concat([ans, counts / 15], axis=1)
    return ans


def TrainBuyMean(traintable):
    """
    将traintable按行求均值，选择持有15天内每天盈亏均值大于0的行，并求出持有15天盈利最大的那天
    :return:TrainBuy(对累计15天数据进行行列均值统计,包含区间内交易次数);
            a('Mark' =1,并添加最大天数列);
            Values(由区间左值与最大天数的组成的ndarray)
    """
    TrainBuy = pd.concat([traintable[traintable.columns[:15]], traintable[traintable.columns[-1]]], axis=1)
    TrainBuy['left'] = [i.left for i in TrainBuy.index]
    TrainBuy = TrainBuy[TrainBuy['left'] > 0]
    del TrainBuy['left']
    TrainBuy.index = [str(i) for i in TrainBuy.index]
    a = pd.DataFrame(TrainBuy.mean(axis=0).astype('int')).T
    a.index = ['MeanCol']
    TrainBuy = pd.concat([TrainBuy, a], sort=False)
    TrainBuy['MeanRow'] = TrainBuy[TrainBuy.columns[:15]].mean(axis=1).astype('int')
    TrainBuy['Mark'] = [np.sign(i) if i > 0 else 0 for i in TrainBuy['MeanRow']]

    a = (TrainBuy[TrainBuy.columns[:15]] / (np.log(range(3, 18)) / np.log(3))).astype('int')
    # for i in a.columns:
    #     a[i][a[i] < a.loc['MeanCol', i]] = np.NaN
    a['Mark'] = TrainBuy['Mark']
    a['MaxDay'] = [a.iloc[j, :].dropna().sort_values(ascending=False).index[0] for j in range(len(a))]
    a['MaxDay'] = a['MaxDay'].replace('Mark', '-')
    a['MeanRow'] = TrainBuy['MeanRow']
    # a.fillna('-', inplace=True)
    a['MaxDay'] = a[a.columns[:15]].apply(pd.Series.idxmax, axis=1).map(
        lambda x: int(''.join(re.findall(r'[0-9]', x))))
    a.loc['MeanCol', 'Mark'] = 1
    a = a[a['Mark'] == 1]
    SelectBuy = a.copy()
    SelectBuy = SelectBuy.iloc[:-1]
    SelectBuy['Left'] = [round(float(i.split(',')[0][1:]), 1) for i in SelectBuy.index]
    Values = SelectBuy[['Left', 'MaxDay']].values
    return TrainBuy, a, Values


def selectEarning(testbuy, values, dates, CutDate):
    """
    :param table: testbuy
    :param values:
    :param dates:
    :param CutDate:
    :return: res(train表中(天数，占比)在test表中对应的数据;counts(res的统计表)
    """
    testbuy = MakeEarningDateTable(testbuy,dates)
    testbuy2 = testbuy[['日期', '盈利日期', '持有天数', '合约代码', '多头占比', '交易盈亏', '累计持仓盈亏', '总盈亏']]
    days = testbuy2['日期'].drop_duplicates().values
    L = []
    for day in days:
        testoneday = testbuy2[testbuy2['日期'] == day]
        ratio = testoneday['多头占比'].iloc[0]
        if ratio in values[:, 0]:
            tempday = values[values[:, 0] == ratio][:, 1][0]
            l = testoneday[testoneday['持有天数'] == tempday]
            L.append(l)
    if len(L) > 0:
        res = pd.concat(L)
        res = res[res['盈利日期'] != "-1"]
        res.index = pd.to_datetime(res['盈利日期'])
        res['累计盈亏'] = res['总盈亏'].cumsum()
        earning = res.pivot_table(index='多头占比', values='总盈亏', aggfunc=np.sum)
        counts = pd.concat(
            [earning, res[['多头占比', '持有天数']].drop_duplicates().set_index('多头占比'), res['多头占比'].value_counts()], axis=1)
        counts = counts.rename(columns={'持有天数': '持有天数', '多头占比': '交易次数'})
        row = counts.apply(func={'总盈亏': sum, '持有天数': np.mean, '交易次数': sum}).apply(round).rename('Describe')
        counts = pd.concat([counts, pd.DataFrame(row).T])
        return res, counts
    else:
        print("没有对应情况")
        return None, None



def main(mySqlDB, company, StartDate, EndDate,CutDate,shift):
    Dates = Mysql_GetDates(mySqlDB, StartDate, EndDate)
    # NameDate = Get_Contracts(ContractName, StartDate, EndDate)
    trainbuy = MySql_GetSeasonDay(mySqlDB, company, StartDate, CutDate, shift)
    testbuy = MySql_GetSeasonDay(mySqlDB, company, CutDate, EndDate, shift)
    table2 = MakeEarning(trainbuy)
    TrainBuy1, SelectBuy1, Values1 = TrainBuyMean(table2)
    TestAns1, TestAnsCounts1 = selectEarning(testbuy, Values1, Dates, CutDate)
    TestAnsCounts1['间隔天数'] = shift
    return TestAnsCounts1

if __name__ == '__main__':
    mySqlDBLocal = ProMySqlDB(mySqlDBC_EARNINGDB_Name, mySqlDBC_UserLocal,
                              mySqlDBC_Passwd, mySqlDBC_HostLocal, mySqlDBC_Port)
    mySqlDBReader = ProMySqlDB(mySqlDBC_DataOIDB_Name, mySqlDBC_User,
                               mySqlDBC_Passwd, mySqlDBC_Host, mySqlDBC_Port)

    ContractName = 'RB.SHF'
    StartDate = '2016-01-01'
    EndDate = '2018-12-31'
    CutDate = '2017-01-01'
    # Dates = Mysql_GetDates(mySqlDBLocal, StartDate, EndDate)
    NameDate = Get_Contracts(mySqlDBReader,ContractName,StartDate,EndDate)
    print(NameDate)
    # TrainBuyList = []
    # for company in Companys:
    #     temp = (MySql_GetSeasonDay(mySqlDBLocal, company, StartDate, CutDate, 1))
    #     if temp is not None:
    #         TrainBuyList.append(temp)
    # trainbuy = MySql_GetSeasonDay(mySqlDBLocal, '永安期货', StartDate, CutDate, 1)
    # testbuy = MySql_GetSeasonDay(mySqlDBLocal, '永安期货', CutDate, EndDate, 1)
    # table2 = MakeEarning(trainbuy)
    # TrainBuy1, SelectBuy1, Values1 = TrainBuyMean(table2)
    # TestAns1, TestAnsCounts1 = selectEarning(testbuy, Values1, Dates, CutDate)
    # print(Values1)
    # print(TestAnsCounts1)
    # Dates = runtimeR(Mysql_GetDates,mySqlDBLocal, StartDate, EndDate)
    # trainbuy = runtimeR(MySql_GetSeasonDay, mySqlDBLocal, '永安期货', StartDate, CutDate, 1)
    # Mysql_CheckSeveralearningDate(mySqlDBLocal)
    MakeSeveralEarning(mySqlDBLocal,mySqlDBReader, ContractName,'2018-08-17','2018-12-31',Companys,'多头占比',5)
    mySqlDBLocal.Close()
    mySqlDBReader.Close()
    #
