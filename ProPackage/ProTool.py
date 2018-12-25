import datetime


def frameToTuple(dataframe):
    """
    将pandas的Dataframe转换成由元组构成元组，用于mysql批量存入
    :param dataframe:
    :return:
    """
    length = len(dataframe)
    array = dataframe.values
    tupleslist = [tuple(array[i, :]) for i in range(length)]
    tuples = tuple(tupleslist)
    return tuples


def runtimeR(func, *args, **kwargs):
    """
    测试带参数_有_返回值的函数的运行时间
    :param func:
    :param args:
    :param kwargs:
    :return:
    """
    start = datetime.datetime.now()
    ans = func(*args, **kwargs)
    end = datetime.datetime.now()
    print(end - start)
    return ans


def runtime(func, *args, **kwargs):
    """
    测试带参数_没有_返回值的函数的运行时间
    :param func:
    :param args:
    :param kwargs:
    :return:
    """
    start = datetime.datetime.now()
    func(*args, **kwargs)
    end = datetime.datetime.now()
    print(end - start)


def Make_Interval_Pecentage(low, high, interval):
    """
    对浮点数划分区间
    :param low:
    :param high:
    :param interval: 0.2
    :return:Interval格式的数组
    """
    low = float("%0.1f" % (pd.np.floor(low / interval) * interval)) * 100
    high = float("%0.1f" % (pd.np.ceil(high / interval) * interval)) * 100
    f = list(range(int(pd.np.floor(np.round(low, 1))), int(pd.np.ceil(np.round(high, 1) + interval * 100)),
                   int(interval * 100)))
    f = [i / 100 for i in f]
    return f
