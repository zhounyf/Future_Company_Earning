"""
将期货多头持仓排名按前5、前10等多档汇总，作为大户持仓指标，以此来回测在年度、品种级别的盈亏情况。
"""
from ProPackage.LangMySQL import *
from ProPackage.ProConfig import *
from ProPackage.ProTool import *
from EarningSimilarCompanys import Get_Contracts
import numpy as np
import re
import matplotlib.pyplot as plt
from pylab import mpl
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)
mpl.rcParams['font.sans-serif'] = ['SimHei']
mpl.rcParams['axes.unicode_minus'] = False
pd.set_option('mode.chained_assignment', None)


def MakeIndexContract(mysqlDB, company, contracts, dates):
    """
    生成comany在时间区间内所有合约的持仓量汇总的情况，一般从1月1号开始计算，
    则01合约在交割月内低持仓量忽略,用于计算相关性
    :param mysqlDB:
    :param company:
    :param contracts:
    :return:
    """
    for contract in contracts:
        Buy = Mysql_GetBuyOI(mysqlDB, '永安期货', contract)
        dates[contract] = Buy['持买仓量']
    dates[company + 'OI'] = dates[contracts].sum(axis=1)
    dates.index.name = company
    return dates


def MakeRankTable(mysqlDB, dates, contractname, limit):
    """
    计算某期货品种每日排名前limit的持买仓量的合计值的时间序列
    :param mysqlDB:
    :param dates:
    :param contractname:
    :param limit:
    :return:
    """
    dates['limit'] = pd.np.nan
    for i in range(len(dates)):
        temp = Mysql_GetRankTopNmaes(mysqlDB, dates.iloc[i,0], contractname, limit)
        if len(temp) > 0:
            # L.append(temp[:, 1].astype(int).sum())
            dates.iloc[i,1] = temp[:, 1].astype(int).sum()
    dates = dates.dropna()
    return dates


def MakeRollingRationkind(contracttable, shifts, limit):
    """
    根据不同间隔天数计算出的多头变化占比值
    :param table: Mysql_GetRankLimit
    :return: shift后的多头变化占比
    """
    contracttable = contracttable.rename(columns={limit: '持买仓量'})
    contracttable['持买增减量'] = contracttable['持买仓量'].diff().fillna(1)
    contracttable['多头占比'] = contracttable['持买仓量'] / contracttable['合约持仓量']
    table = contracttable.copy()
    table['持买仓量shift'] = table['持买仓量'].shift(shifts)
    table['持买增减量求和'] = table['持买增减量'].rolling(shifts).sum()
    table['合约持仓量shift'] = table['合约持仓量'].shift(1)
    table['多头变化占比'] = (table['持买增减量求和'] / table['合约持仓量shift']).map(lambda x: float("%.1f" % (x * 100)))
    contracttable['多头占比'] = table['多头变化占比']
    contracttable.rename(columns={'多头占比': '多头变化占比'}, inplace=True)
    return contracttable


def MakeEarningTableOneDay(table, ratio, day, multiplierTable):
    indexes_ = table[table['多头变化占比'] == ratio].index
    muti = multiplierTable['合约乘数'][table['期货品种'].values[0]]
    ans = []
    for index_ in indexes_:
        if index_ == table.index[-1]:
            pass
        else:
            T = table.loc[index_:(index_ + day)].copy()
            contract = T['期货品种'].iloc[0]
            date = T['日期'].iloc[0]
            buyValue = T['持买仓量'].iloc[0]
            changValue = T['持买增减量'].iloc[0]
            totalValue = T['合约持仓量'].iloc[0]
            exchangeEarning = pd.np.sign(T['持买增减量'].iloc[0]) * (T['收盘价'].iloc[1] - T['开盘价'].iloc[1]) * muti
            T['收盘价'].iloc[0] = pd.np.nan
            holdEarning = sum(pd.np.sign(T['持买增减量'].iloc[0]) * T['收盘价'].diff().fillna(0)) * muti
            totalEarning = exchangeEarning + holdEarning
            L = [day, contract, ratio, date, buyValue, changValue, totalValue, exchangeEarning, holdEarning,
                 totalEarning]
            ans.append(L)
    ansTable = pd.DataFrame(ans, columns=['持有天数', '期货品种', '多头变化占比', '日期', '持买仓量',
                                          '持买增减量', '合约持仓量', '交易盈亏', '累计持仓盈亏', '总盈亏'])
    return ansTable


def MakeEarningTableSeveralDay(table, days, multiplierTable):
    table.index = range(len(table))
    ratios = sorted(list(table['多头变化占比'].drop_duplicates()))
    T = []
    for day in list(range(1, days)):
        for ratio in ratios:
            temp = MakeEarningTableOneDay(table, ratio, day, multiplierTable)
            T.append(temp)
    ansTable = pd.concat(T)
    ansTable = ansTable.sort_values('日期')
    return ansTable


def MakeDaysEarning(table):
    table = table[table['多头变化占比'] > 0]
    L = []
    value = sorted(table['多头变化占比'].drop_duplicates().values)
    interval = Make_Interval_Pecentage(value[0], value[-1], 0.2)
    table['interval'] = pd.cut(list(table['多头变化占比']), interval, right=False)
    for day in range(1, 16):
        A = table[table['持有天数'] == day]
        A['interval'] = pd.cut(list(A['多头变化占比']), interval, right=False)
        l = A.groupby(by='interval').mean().dropna()['总盈亏']
        l = l.rename(columns={'总盈亏': day})
        L.append(l)
    ans = pd.concat(L, axis=1).astype('int')
    ans['left'] = [i.left for i in ans.index]
    ans['right'] = [i.right for i in ans.index]
    ans['期货品种'] = table['期货品种'].iloc[0]
    ans.columns = ['day1', 'day2', 'day3', 'day4', 'day5', 'day6', 'day7', 'day8', 'day9', 'day10',
                   'day11', 'day12', 'day13', 'day14', 'day15', 'interleft', 'interright', '期货品种']
    counts = pd.DataFrame(table['interval'].value_counts())
    counts.columns = ['counts']
    counts = counts[counts['counts'] != 0]
    ans = pd.concat([ans, counts / 15], axis=1)
    return ans


def TrainBuyMean(traintable):
    """
    将traintable按行求均值，选择持有15天内每天盈亏均值大于0的行，并求出持有15天盈利最大的那天
    :return:TrainBuy(对累计15天数据进行行列均值统计,包含区间内交易次数);
            Marked('Mark' =1,并添加最大天数列);
            Values(由区间左值与最大天数的组成的ndarray)
    """
    TrainBuy = pd.concat([traintable[traintable.columns[:15]], traintable[traintable.columns[-1]]], axis=1)
    TrainBuy['left'] = [i.left for i in TrainBuy.index]
    TrainBuy = TrainBuy[TrainBuy['left'] >= 0]
    del TrainBuy['left']
    TrainBuy.index = [str(i) for i in TrainBuy.index]
    a = pd.DataFrame(TrainBuy.mean(axis=0).astype('int')).T
    a.index = ['MeanCol']
    TrainBuy = pd.concat([TrainBuy, a], sort=False)
    TrainBuy['MeanRow'] = TrainBuy[TrainBuy.columns[:15]].mean(axis=1).astype('int')
    TrainBuy['Mark'] = [np.sign(i) if i > 0 else 0 for i in TrainBuy['MeanRow']]

    Marked = (TrainBuy[TrainBuy.columns[:15]] / (np.log(range(3, 18)) / np.log(3))).astype('int')
    Marked['Mark'] = TrainBuy['Mark']
    Marked['MaxDay'] = [Marked.iloc[j, :].dropna().sort_values(ascending=False).index[0] for j in range(len(Marked))]
    Marked['MaxDay'] = Marked['MaxDay'].replace('Mark', '-')
    Marked['MeanRow'] = TrainBuy['MeanRow']
    Marked['MaxDay'] = Marked[Marked.columns[:15]].apply(pd.Series.idxmax, axis=1).map(
        lambda x: int(''.join(re.findall(r'[0-9]', x))))
    Marked.loc['MeanCol', 'Mark'] = 1
    Marked = Marked[Marked['Mark'] == 1]
    SelectBuy = Marked.copy()
    SelectBuy = SelectBuy.iloc[:-1]
    SelectBuy['Left'] = [round(float(i.split(',')[0][1:]), 1) for i in SelectBuy.index]
    Values = SelectBuy[['Left', 'MaxDay']].values
    return TrainBuy, Marked, Values


def MakeEarningDateTable(table, Dates, endDate):
    """
    用开仓前日期和持有天数推算盈利日期，去掉超过测试
    :param table:
    :param Dates:
    :return:
    """
    Dates.index = range(len(Dates))
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
    table = table[table['盈利日期'] < endDate]
    return table


def selectEarning(testbuy, values, dates, enddate):
    """
    :param table: testEarning
    :return: res(train表中(天数，占比)在test表中对应的数据;counts(res的统计表)
    """
    values = values[values[:, 0] > 0]
    testbuy = MakeEarningDateTable(testbuy, dates, enddate)
    testbuy2 = testbuy[['日期', '盈利日期', '持有天数', '多头变化占比', '交易盈亏', '累计持仓盈亏', '总盈亏']]
    days = testbuy2['日期'].drop_duplicates().values
    L = []
    for day in days:
        testoneday = testbuy2[testbuy2['日期'] == day]
        ratios = testoneday['多头变化占比'].drop_duplicates().values
        for ratio in ratios:
            if ratio in values[:, 0]:
                tempday = values[values[:, 0] == ratio][:, 1][0]
                l = testoneday[testoneday['持有天数'] == tempday]
                L.append(l)
    if len(L) > 0:
        res = pd.concat(L)
        res = res[res['盈利日期'] != "-1"]
        res = res[res['多头变化占比'] > 0]
        if len(res) > 0:
            res.index = pd.to_datetime(res['盈利日期'])
            res['累计盈亏'] = res['总盈亏'].cumsum()
            earning = res.pivot_table(index='多头变化占比', values='总盈亏', aggfunc=np.sum)
            # if -0.0 in earning.index.values:
            #     counts = pd.concat(
            #         [earning.drop(-0, axis=0),
            #          res[['多头变化占比', '持有天数']].drop_duplicates().set_index('多头变化占比').drop(-0, axis=0),
            #          res['多头变化占比'].value_counts().drop(-0, axis=0)], axis=1)
            # else:
            try:
                counts = pd.concat(
                    [earning,res[['多头变化占比', '持有天数']].drop_duplicates().set_index('多头变化占比'),
                     res['多头变化占比'].value_counts()], axis=1)
            except ValueError:
                print("value wrong")
                return None, None
            else:
                counts = counts.rename(columns={'持有天数': '持有天数', '多头变化占比': '交易次数'})
                if len(counts) > 0:
                    row = counts.apply(func={'总盈亏': sum, '持有天数': np.mean, '交易次数': sum}).apply(round).rename('Describe')
                    counts = pd.concat([counts, pd.DataFrame(row).T])
                    return res, counts
                else:
                    print("counts is none")
                    return None, None
        else:
            print("持有天数均未达到！")
            return None, None
    else:
        print("没有对应情况")
        return None, None


def TotalOperate(mySqlDB, trainstart, trainend, teststart, testend, contractname, limit, shifts, *company):
    TrainStartDate = pd.datetime.strptime(trainstart, "%Y-%m-%d") + pd.Timedelta("-20 days")
    TrainDate = pd.datetime.strftime(TrainStartDate, "%Y-%m-%d")
    TestStartDate = pd.datetime.strptime(teststart, "%Y-%m-%d") + pd.Timedelta("-20 days")
    TestDate = pd.datetime.strftime(TestStartDate, "%Y-%m-%d")

    if len(company) == 0:
        TrainTable = Mysql_GetRankLimit(mySqlDB, contractname.split(".")[0], TrainDate, trainend, limit)
    else:
        TrainTable = Mysql_GetOneCompanyRank(mySqlDB, contractname, company[0], TrainDate, trainend)
    # TrainTable = TrainTable[trainstart:trainend]
    if TrainTable is not None:

        TrainRationTable = MakeRollingRationkind(TrainTable, shifts, limit)

        if len(company) == 0:
            TestTable = Mysql_GetRankLimit(mySqlDB, contractname.split(".")[0], TestDate, testend, limit)
        else:
            TestTable = Mysql_GetOneCompanyRank(mySqlDB, contractname, company[0], TestDate, testend)
        # TestTable = TestTable[teststart:testend]
        TestRationTable = MakeRollingRationkind(TestTable, shifts, limit)

        TrainTableEarning = MakeEarningTableSeveralDay(TrainRationTable, 16, MultiplierTable)
        TrainTableEarning.index = pd.to_datetime(TrainTableEarning['日期'])
        trainEarning = TrainTableEarning[trainstart:trainend]

        TestTableEarning = MakeEarningTableSeveralDay(TestRationTable, 16, MultiplierTable)
        TestTableEarning.index = pd.to_datetime(TestTableEarning['日期'])
        testEarning = TestTableEarning[teststart:testend]

        TrainBuy, Marked, Values = TrainBuyMean(MakeDaysEarning(trainEarning))
        Dates = Mysql_GetDates(mySqlDB, trainstart, testend)
        res, counts = selectEarning(testEarning, Values, Dates, testend)
        if counts is not None:
            counts.insert(0, 'left', counts.index)
            counts['前多少排名'] = ''.join(re.findall(r'[0-9]', limit))
            counts['观察天数'] = shifts
            counts['训练开始日期'] = trainstart
            counts['训练结束日期'] = trainend
            counts['测试开始日期'] = teststart
            counts['测试结束日期'] = testend
            counts['品种'] = contractname.split(".")[0]
            values = frameToTuple(counts)
            # print(counts)
            MySql_BatchInsertRankEarningAnswer(mySqlDB, values)
            print("{name},{limit},{shifts} Insert Done".format(name=contractname.split(".")[0], limit=limit, shifts=shifts))

    else:
        print("{name} 没有数据".format(name=contractname.split(".")[0]))


def AnswerTest(traintable, mysqlDB, contarctname, TestStart, TestEnd):
    """
    traintable AnalysisFutureRank中得出的结论，与test日期内的数据比较
    :param traintable:
    :param mysqlDB:
    :param contarctname:
    :param TestStart:
    :param TestEnd:
    :return:
    """
    T = []
    Limits = []
    for i in range(1, 21):
        Limits.append('limit' + str(i))
    for limit in Limits:
        for shift in range(1, 6):
            # TotalOperate(mySqlDBLocal,TrainStart,TrainEnd,TestStart,TestEnd,ContractName,limit,shift)
            TrainStartDate = pd.datetime.strptime(TestStart, "%Y-%m-%d") + pd.Timedelta("-20 days")
            TrainDate = pd.datetime.strftime(TrainStartDate, "%Y-%m-%d")
            TrainTable = Mysql_GetRankLimit(mysqlDB, contarctname.split(".")[0], TrainDate, TestEnd, limit)
            TrainRationTable = MakeRollingRationkind(TrainTable, 1, limit)
            TrainRationTable = TrainRationTable[TestStart:TestEnd]
            TrainTableEarning = MakeEarningTableSeveralDay(TrainRationTable, 16, MultiplierTable)
            TrainTableEarning.index = pd.to_datetime(TrainTableEarning['日期'])
            Dates = Mysql_GetDates(mysqlDB, TestStart, TestEnd)
            TrainTableEarning = MakeEarningDateTable(TrainTableEarning, Dates, TestEnd)
            TrainTableEarning['前多少排名'] = int(limit.split('t')[1])
            TrainTableEarning['观察天数'] = int(shift)
            T.append(TrainTableEarning)

    traintable['Answer'] = traintable['观察天数'].astype('str') + '_' + traintable['区间左值'].astype('str') + '_' + \
                           traintable['持有天数'].astype('str') + '_' + traintable['前多少排名'].astype('str')
    t = pd.concat(T)
    t['Answer'] = t['观察天数'].astype('str') + '_' + t['多头变化占比'].astype('str') + '_' \
                  + t['持有天数'].astype('str') + '_' + t['前多少排名'].astype('str')

    temp = []
    for i in traintable['Answer'].values:
        temp.append(t[t['Answer'] == i])

    return pd.concat(temp)


def MakeEarningDateTable2(table, Dates, endDate, worstday):
    """
    用开仓前日期和持有天数推算盈利日期，去掉超过测试
    :param table:
    :param Dates:
    :return:
    """
    Dates.index = range(len(Dates))
    DatesV = Dates.values
    tablebuyV = table[['持有天数', '日期']].values
    newdays = []
    for temp in tablebuyV:
        if temp[1] not in worstday:
            testdayindex = ''
            for i in range(1, temp[0] + 1):
                testdayindex = Dates[Dates['Dates'] == temp[1]].index + i
                try:
                    testday = DatesV[testdayindex][0][0]
                    if testday in worstday:
                        newdays.append(testday)
                        break
                    elif i == temp[0]:
                        newdays.append(DatesV[testdayindex][0][0])
                    else:
                        continue
                #                     newdays.append(testday)

                except:
                    newdays.append('-1')
                    break

        else:
            newdays.append('-2')

    table['盈利日期'] = newdays
    table = table[table['盈利日期'] < endDate]
    return table


# if __name__ == '__main__':
#     mySqlDBLocal = ProMySqlDB(mySqlDBC_EARNINGDB_Name, mySqlDBC_UserLocal,
#                               mySqlDBC_Passwd, mySqlDBC_HostLocal, mySqlDBC_Port)
#
#     # table = Mysql_GetTestDateGroup(mySqlDBLocal)
#     datetable = pd.read_csv(".\doc\datetable.csv",names = ["TrainStart","TrainEnd","TestStart","TestEnd"])
#     for ContractName in ["L.DCE"]:
#         for i in range(0, len(datetable)):
#             dates = datetable.iloc[i].values
#             TrainStart, TrainEnd, TestStart, TestEnd = dates
#             print(TrainStart, TrainEnd, TestStart, TestEnd)
#
#             # ans = TotalOperate(mySqlDBLocal, TrainStart, TrainEnd, TestStart, TestEnd, ContractName,'limit1', 1,'永安期货')
#             # print(ans)
#             T = []
#             Limits = []
#             for i in range(1, 21):
#                 Limits.append('limit' + str(i))
#             for limit in Limits:
#                 for shift in range(1, 6):
#             # for limit in ['limit12']:
#             #     for shift in [3]:
#
#                     # 计算RankEarningAnswer
#                     TotalOperate(mySqlDBLocal, TrainStart, TrainEnd, TestStart, TestEnd, ContractName, limit, shift)
#     #
#     mySqlDBLocal.Close()
#     print(pd.datetime.now())

# if __name__ == '__main__':
#     mySqlDBLocal = ProMySqlDB(mySqlDBC_EARNINGDB_Name, mySqlDBC_UserLocal,
#                                   mySqlDBC_Passwd, mySqlDBC_HostLocal, mySqlDBC_Port)
#     pricetable = Mysql_GetBuyOIIndex(mySqlDBLocal, 'RB.SHFE', '2018-01-01', '2018-12-31')
#     table2018 = pd.read_csv('doc/table2018.csv', engine='python', index_col=0, parse_dates=True)
#     table2018['日期'] = pd.to_datetime(table2018['日期']).apply(lambda x: pd.datetime.strftime(x, "%Y-%m-%d"))
#     pricetable['MA'] = pricetable['结算价'].rolling(10).mean()
#     pricetable['diff'] = pricetable['MA'].diff(4) / 4
#     wrongdays = pricetable[pricetable['diff'] < -5]['日期'].values
#     Dates = Mysql_GetDates(mySqlDBLocal, '2018-01-01', '2018-12-31')
#     a = MakeEarningDateTable2(table2018, Dates, '2018-12-31',wrongdays)


def StrategySignal():
    j = 0
    L = []
    Dates = pd.read_csv('.\doc\Dates.csv').values
    Names = os.listdir('.\doc\sign')
    for name in Names:
        t = pd.read_csv('.\doc\sign\{name}'.format(name=name),engine='python')
        # FRealContract = t['期货品种'].iloc[0,0].lower()+'0000'
        T = t[['日期', '持有天数','盈利日期','期货品种']]
        for i in range(len(T)):
            tempdate = T.iloc[i, 0]
            tempindex = T.iloc[i, 1]
            newindex = np.where(Dates == tempdate)[0][0] + tempindex
            T.iloc[i, 2] = Dates[newindex][0]+ ' 20:58:00'
            T.iloc[i, 0] = T.iloc[i, 0] + ' 20:58:00'
        L.append(T)
    Ans = pd.concat(L)
    Ans.insert(1,'BUY','Buy')
    Ans.insert(4, 'SELL', 'Sell')
    return Ans

def TransformSignal(ans):
    Names = pd.read_csv("./doc/contracttype.csv", engine='python').values
    Dict = {key:value for key,value in Names}

    tempbuy = ans[['日期', 'BUY','期货品种']]
    tempbuy = tempbuy.rename(columns={'日期': 'FSignalTime', 'BUY': 'FSide','期货品种':'FRealContract'})
    tempbuy['FSignalDay'] = [i[:10] for i in tempbuy['FSignalTime']]
    tempbuy['FStrategyName'] = ['FutureOIRankZYF0001'+ Dict[i]+'0000000000' for i in tempbuy['FRealContract']]
    tempbuy['FRealContract'] =[ Dict[i] + '0000' for i in tempbuy['FRealContract']]

    tempsell = ans[['盈利日期', 'SELL','期货品种']]
    tempsell = tempsell.rename(columns={'盈利日期': 'FSignalTime', 'SELL': 'FSide','期货品种':'FRealContract'})
    tempsell['FSignalDay'] = [i[:10] for i in tempsell['FSignalTime']]
    tempsell['FStrategyName'] = ['FutureOIRankZYF0001'+ Dict[i]+'0000000000' for i in tempsell['FRealContract']]
    tempsell['FRealContract'] = [ Dict[i] + '0000' for i in tempsell['FRealContract']]

    temp = pd.concat([tempbuy,tempsell])
    temp['FTradedVolume'] = 1
    temp['FSpecificPrice'] = None
    temp['FTradedDay'] = None
    temp['FTradedPrice'] = None
    return temp[['FStrategyName','FRealContract','FSide','FTradedVolume',
                 'FSpecificPrice','FTradedDay','FTradedPrice','FSignalTime','FSignalDay']]


if __name__ == '__main__':
    mySqlDBLocal = ProMySqlDB(mySqlDBC_EARNINGDB_Name, mySqlDBC_UserLocal,
                              mySqlDBC_Passwd, mySqlDBC_HostLocal, mySqlDBC_Port)

    a = StrategySignal()
    b = TransformSignal(a)
    b.to_csv('FutureRankSignal.csv',encoding='GBK',index=None)
    # print(table.iloc[0,:].split(' '))
