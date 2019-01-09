from ProPackage.LangMySQL import *
from ProPackage.ProConfig import *
from pyecharts import Line, Grid, Overlap
import numpy as np


def CorrelationTable(mySqlDB, ratiokind, companys, contract, start, end):
    """
    计算与永安期货在活跃合约区间中相关性大于0.5的期货公司
    :param mySqlDB:local
    :param ratiokind:'持买仓量'
    :param companys:ALL Companys
    :param contract:Name_Date中的
    :param start:
    :param end:
    :return: table(所有期货公司在区间内的ratiokind),
            corr(table中ratiokind列均值大于所有列均值的期货公司与永安期货相关性大于0.5的期货公司)
    """
    columnDataList = []
    for company in companys:
        temp = Mysql_GetColumnData(mySqlDB, ratiokind, company, contract, start, end)
        if temp is not None:
            columnDataList.append(temp)

    # table = pd.concat(columnDataList, axis=1)
    # names = table.mean()[table.mean() > table.mean().mean()].index.values
    # corrtable = table[names]
    # corr = corrtable.corrwith(corrtable['永安期货']).sort_values(ascending=False)
    # corr = pd.DataFrame(corr, columns=['相关性'])
    # corr = corr[corr['相关性'] > 0.0]
    # corr['合约代码'] = contract
    # corrtable = table[corr.index]
    # return corrtable, corr
    return columnDataList


def Get_Contracts(mySqlDB, contractname, start, end):
    """
    计算连续的主力合约名称已经开始结束日期
    :param contractname:'RB.SHF'
    :param start:
    :param end:
    :return:
    """
    contractTable = Mysql_GetMainContracts(mySqlDB,contractname)
    ans = contractTable[start:end]
    names = ans['合约代码'].drop_duplicates().values
    nameDate = []
    for i in names:
        datestart = pd.datetime.strftime(ans[ans['合约代码'] == i].index[0], '%Y-%m-%d')
        dateend = pd.datetime.strftime(ans[ans['合约代码'] == i].index[-1], '%Y-%m-%d')
        nameDate.append((datestart, dateend, i))
    nameDate = np.array(nameDate)
    return nameDate


def GetClosePrice(mysqlDB, contract, start, end):
    contractData = MySql_GetContractData_Earning(mysqlDB, contract).drop_duplicates(subset=['日期'])
    price = contractData.set_index(pd.to_datetime(contractData['日期']))['收盘价']
    closePrice = price[start: end]
    return closePrice


def PlotCompare(corrtable, closeprice,contract):
    """
    用pyecharts绘制期货公司持买仓量与合约价格走势的对比图
    :param corrtable:
    :param closeprice:
    :return:
    """
    line = Line("期货公司持买仓量与合约价格走势的对比图", height=800, width=1200)
    for j in range(len(corrtable.columns)):
        dfvalue = [i for i in corrtable[corrtable.columns[j]].values]
        _index = [i for i in corrtable.index.format()]
        line.add(corrtable.columns[j], _index, dfvalue, line_width=2, is_datazoom_show=True, xaxis_rotate=30,
                 legend_top='bottom')

    line2 = Line(height=800, width=1200)
    dfvalue = [i for i in closeprice.values]
    _index = [i for i in closeprice.index.format()]
    line2.add('收盘价', _index, dfvalue, is_datazoom_show=True, xaxis_rotate=30, line_width=3,
              is_splitline_show=False, yaxis_min=closeprice.min(), line_color='b', is_symbol_show=True,
              legend_top='bottom')
    overlap = Overlap(width=1200, height=600)
    overlap.add(line)
    overlap.add(line2, is_add_yaxis=True, yaxis_index=1)
    grid = Grid()
    grid.add(overlap, grid_bottom="25%")
    path = os.path.dirname(os.getcwd()) + os.sep + 'image' + os.sep
    grid.render(path + '永安期货%s持买仓量与合约价格走势的对比图.html' % contract)

def MakeCorrTable(Lcorr):
    """
    将CorrelationTable计算的各家期货公司对永安期货的持仓量相关性的数据合并
    :param Lcorr: corr
    :return:
    """
    temp0 = Lcorr[0]
    for i in range(1, len(Lcorr)):
        temp0 = pd.merge(temp0, Lcorr[i], how='outer', left_index=True, right_index=True)
    corrcolumns = list(range(0,len(Lcorr)*2,2))
    contractcolumns = list(range(1,len(Lcorr)*2+1 ,2))
    corrnames = temp0.iloc[:, contractcolumns].fillna(method='ffill').iloc[-1, :].values
    newtemp = temp0.iloc[:, corrcolumns]
    newtemp.columns = corrnames
    return newtemp


if __name__ == '__main__':
    mySqlDBLocal = ProMySqlDB(mySqlDBC_EARNINGDB_Name, mySqlDBC_UserLocal,
                              mySqlDBC_Passwd, mySqlDBC_HostLocal, mySqlDBC_Port)
    mySqlDBReader = ProMySqlDB(mySqlDBC_DataOIDB_Name, mySqlDBC_User,
                               mySqlDBC_Passwd, mySqlDBC_Host, mySqlDBC_Port)
    Company = ['永安期货']
    ContractName = 'RB.SHF'
    StartDate = '2017-01-01'
    EndDate = '2018-12-31'
    CutDate = '2017-01-01'
    Dates = Mysql_GetDates(mySqlDBLocal, StartDate, EndDate)
    Dates.index = pd.to_datetime(Dates['Dates'])
    # DayInterval = 1
    # Name_Date = Get_Contracts(mySqlDBReader, ContractName, StartDate, EndDate)
    # L = []
    # for i in range(len(Name_Date)):
    #     contract = Name_Date[i][2]
    #     start = Name_Date[i][0]
    #     end = Name_Date[i][1]
    #
    #     # closePrice = GetClosePrice(mySqlDBLocal, contract, start, end)
    #     correlationTable = CorrelationTable(mySqlDBLocal, '持买仓量', Company, contract, start, end)
    #     # PlotCompare(correlationTable, closePrice, contract)
    #     L.append(correlationTable)
#     ans = MakeCorrTable(L)
#     print(ans)
#     yn01 = Mysql_GetBuyOI(mySqlDBLocal, '永安期货','rb1701')
    # yn05 = Mysql_GetBuyOI(mySqlDBLocal, '永安期货','rb1705')
    # yn09 = Mysql_GetBuyOI(mySqlDBLocal, '永安期货','rb1710')
    # print(Mysql_GetRankTopNmaes(mySqlDBReader,'2018-12-04' ,'rb1901' ,5))
    # mySqlDBLocal.Close()
    # mySqlDBReader.Close()