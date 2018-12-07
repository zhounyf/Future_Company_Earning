import datetime


def frameToTuple(dataframe):
    L = len(dataframe)
    Array = dataframe.values
    tupleList = [tuple(Array[i, :]) for i in range(L)]
    ans = tuple(tupleList)
    return ans


def runtimeR(func, *args, **kwargs):
    start = datetime.datetime.now()
    ans = func(*args, **kwargs)
    end = datetime.datetime.now()
    print(end - start)
    return ans


def runtime(func, *args, **kwargs):
    start = datetime.datetime.now()
    func(*args, **kwargs)
    end = datetime.datetime.now()
    print(end - start)
