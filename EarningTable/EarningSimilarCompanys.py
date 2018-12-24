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

    table = pd.concat(columnDataList, axis=1)
    names = table.mean()[table.mean() > table.mean().mean()].index.values
    corrtable = table[names]
    corr = corrtable.corrwith(corrtable['永安期货']).sort_values(ascending=False)
    corr = corr[corr > 0.5]
    corrtable = table[corr.index]
    return corrtable, corr


def Get_Contracts(contract, start, end):
    """
    通过主力合约.xlsx,计算连续的主力合约名称已经开始结束日期
    :param contract:
    :param start:
    :param end:
    :return:
    """
    path = os.path.dirname(os.getcwd()) + os.sep + 'doc' + os.sep + '主力合约.xlsx'
    contractTable = pd.read_excel(path)
    RB = contractTable[contract][start:end]
    names = RB.drop_duplicates().values
    nameDate = []
    for i in names:
        datestart = pd.datetime.strftime(RB[RB == i].index[0], '%Y-%m-%d')
        dateend = pd.datetime.strftime(RB[RB == i].index[-1], '%Y-%m-%d')
        nameDate.append((datestart, dateend, i[:-4]))
    nameDate = np.array(nameDate)
    for i in range(1, len(nameDate)):
        nameDate[i, 0] = nameDate[i - 1, 1]
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


# if __name__ == '__main__':
#
#     Company = ['永安期货']
#     ContractName = 'RB.SHF'
#     StartDate = '2016-01-01'
#     EndDate = '2017-12-31'
#     CutDate = '2017-01-01'
#     DayInterval = 1
#     Name_Date = Get_Contracts(ContractName, StartDate, EndDate)
#
#     for i in range(len(Name_Date)):
#         contract = Name_Date[i][2]
#         start = Name_Date[i][0]
#         end = Name_Date[i][1]
#
#         closePrice = GetClosePrice(mySqlDBLocal, contract, start, end)
#         correlationTable, corr = CorrelationTable(mySqlDBLocal, '持买仓量', Companys, contract, start, end)
#         PlotCompare(correlationTable, closePrice, contract)

