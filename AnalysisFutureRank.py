from ProPackage.LangMySQL import *
from ProPackage.ProMySqlDB import ProMySqlDB
from ProPackage.ProConfig import *


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


if __name__ == '__main__':
    mySqlDBLocal = ProMySqlDB(mySqlDBC_EARNINGDB_Name, mySqlDBC_UserLocal,
                              mySqlDBC_Passwd, mySqlDBC_HostLocal, mySqlDBC_Port)
    table = Mysql_GetRankEarningAnswer(mySqlDBLocal)
    dates = table[['训练开始日期', '训练结束日期', '测试开始日期', '测试结束日期']].drop_duplicates()
    dates = dates.sort_values(by=['训练开始日期', '测试开始日期'])
    dates.index = range(len(dates))
    trainrow = 4
    testrow = 7
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
