from ProPackage.LangMySQL import *
from ProPackage.ProMySqlDB import ProMySqlDB
from ProPackage.ProConfig import *
from EarningFutureRank import AnswerTest
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
pd.set_option('mode.chained_assignment', None)
from pyecharts import Kline
from pyecharts import Scatter,Overlap,online



def dateparse(date):
    return pd.datetime.strftime(date, '%Y-%m-%d')


def pretreat(table):
    table['单笔收益'] = (table['总盈亏'] / table['交易次数']).astype('int')
    table['训练开始日期'] = table['训练开始日期'].apply(lambda x: dateparse(x))
    table['训练结束日期'] = table['训练结束日期'].apply(lambda x: dateparse(x))
    table['测试开始日期'] = table['测试开始日期'].apply(lambda x: dateparse(x))
    table['测试结束日期'] = table['测试结束日期'].apply(lambda x: dateparse(x))
    return table


def get_common_observation(table,observeday, trainfirststart, trainfirstend, testfirststart, testfirstend,
                           trainlaststart, trainlastend, testlaststart, testlastend):
    earningtotal = table[(table['训练开始日期'] == trainfirststart) & (table['训练结束日期'] == trainfirstend) &(table['测试开始日期'] == testfirststart) &(table['测试结束日期'] == testfirstend)]
    earningtotalgain = earningtotal[earningtotal['单笔收益'] > 0]
    earningtotalgain = earningtotalgain[earningtotalgain['区间左值'] != 'Describe']
    observation = earningtotalgain[earningtotalgain['观察天数'] == observeday]
    lefts = observation['区间左值'].drop_duplicates().values
    L = []
    for left in lefts:
        temp = observation[observation['区间左值'] == left]
        testvalue = temp[['持有天数', '前多少排名']].values
        testtable = table[(table['训练开始日期'] == trainlaststart) &
                          (table['训练结束日期'] == trainlastend) &
                          (table['测试开始日期'] == testlaststart) &
                          (table['测试结束日期'] == testlastend)]
        testobservation = testtable[testtable['观察天数'] == observeday]
        for i in range(len(testvalue)):
            anstable = testobservation[(testobservation['区间左值'] == left) &
                                       (testobservation['持有天数'] == testvalue[i][0]) &
                                       (testobservation['前多少排名'] == testvalue[i][1])]
            if len(anstable) != 0:
                L.append(anstable)
    if len(L) > 0:
        AnsTable = pd.concat(L)
        return AnsTable[AnsTable['单笔收益']>100]
    else:
        print("没有匹配")


def Kind(kind):
    mySqlDBLocal = ProMySqlDB(mySqlDBC_EARNINGDB_Name, mySqlDBC_UserLocal,
                              mySqlDBC_Passwd, mySqlDBC_HostLocal, mySqlDBC_Port)
    table = Mysql_GetRankEarningAnswer(mySqlDBLocal,kind)
    dates = table[['训练开始日期', '训练结束日期', '测试开始日期', '测试结束日期']].drop_duplicates()
    dates = dates.sort_values(by=['训练开始日期', '测试开始日期'])
    dates.index = range(len(dates))
    trainrow = 3
    testrow = 5
    trainfirststart = dateparse(dates.iloc[trainrow,0])
    trainfirstend = dateparse(dates.iloc[trainrow,1])
    testfirststart= dateparse(dates.iloc[trainrow,2])
    testfirstend = dateparse(dates.iloc[trainrow,3])

    trainlaststart = dateparse(dates.iloc[testrow,0])
    trainlastend = dateparse(dates.iloc[testrow,1])
    testlaststart = dateparse(dates.iloc[testrow,2])
    testlastend = dateparse(dates.iloc[testrow,3])

    ObservationTable = pretreat(table)
    L = []
    for day in range(1,6):
        AnsTable = get_common_observation(ObservationTable, day, trainfirststart, trainfirstend, testfirststart,
                                          testfirstend,
                                          trainlaststart, trainlastend, testlaststart, testlastend)
        if AnsTable is not None:
            L.append(AnsTable[['观察天数','区间左值','持有天数','前多少排名','单笔收益']])

    if len(L)>0:
        Ans = pd.concat(L)
        Ans['训练日期段'] = trainfirststart+'_'+trainfirstend+'_'+testfirststart+'_'+testfirstend
        Ans['测试日期段'] = trainlaststart + '_' + trainlastend + '_' + testlaststart + '_' + testlastend
    mySqlDBLocal.Close()

    return Ans


def PactureGenerate(mySqlDB):

    Dates = [['2017-01-01','2017-12-31'],['2018-01-01','2018-12-31']]
    Kinds = ['HC']
    for kind in Kinds:
        for date in Dates:
            start ,end= date
            ans = Kind(kind)
            anstable = AnswerTest(ans, mySqlDBLocal, kind, start, end)
            anstable.to_csv("{name}{start}{end}.csv".format(name=kind,start=start,end=end), encoding='GBK')
            # table = Mysql_GetBuyOIIndex(mySqlDB, kind, '2016-01-01', '2018-12-31')
            #
            # temptable = anstable.copy()
            # temptable = temptable[temptable['盈利日期'] != '-1']
            # temptable.index = pd.to_datetime(temptable['盈利日期'])
            # temptable = temptable.sort_index()
            # table = table.join(temptable['总盈亏'])
            # table = table.fillna(0)
            # table['累计总盈亏'] = table['总盈亏'].cumsum()
            year = start[:4]
            # newtable = table[year]
            # ax = newtable['累计总盈亏'].plot(title='{name}-{start}年开仓然后持有至到期天之后的'
            #                                 '资金曲线'.format(name=kind,start = year), grid=True)
            # ax.get_figure().savefig("./image/{name}-{start}-{end}.png".format(name=kind,start=start,end=end))
            # ax.cla()
            #
            # x = newtable['日期'].values
            # y = newtable[['开盘价', '收盘价', '最低价', '最高价']].values
            # x2 = temptable['日期'].values
            # y2 = [newtable[newtable['日期'] == day]['最高价'].values * 1.02 for day in x2]
            # kline = Kline("{name}指数".format(name=kind))
            # kline.add("{name}{year}日线".format(name=kind,year=year), x, y, is_datazoom_show=True)
            # scatter = Scatter()
            # scatter.add("开仓信号", x2, y2, is_datazoom_show=True)
            # overlap = Overlap()
            # overlap.add(kline)
            # overlap.add(scatter)
            # overlap.render("./image/{name}{year}开仓信号.html".format(name=kind,year=year))
            # newtable.to_csv("./doc/{name}table{year}.csv".format(name=kind,year=year),encoding='GBK')
            # print("交易次数： "+ str(len(temptable)))
            print("{name}{year}完成计算".format(name=kind,year=year))



if __name__ == '__main__':
    # print(len(Kind('I')))
    mySqlDBLocal = ProMySqlDB(mySqlDBC_EARNINGDB_Name, mySqlDBC_UserLocal,
                              mySqlDBC_Passwd, mySqlDBC_HostLocal, mySqlDBC_Port)
    Ans = Kind("AP")
    # anstable = AnswerTest(Ans, mySqlDBLocal, "RB",'2017-01-01','2017-12-31')
    # anstable.to_csv("RB2017-01-01_2017-12-31.csv",encoding='GBK')
    # from EarningFutureRank import AnswerTest
    #
    # AnswerTest(Ans, mySqlDBLocal, 'I.DCE', '2019-01-01', '2019-01-21')
