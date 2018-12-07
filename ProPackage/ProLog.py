# -*- coding: utf-8 -*-
"""
Created on Tue Oct 23 10:25:05 2018

@author: Administrator
"""

import os
from datetime import datetime


class ProLog():
    def __init__(self, logName, fileName='./log/', bufferNumber=10):
        self.__logName = logName
        self.__fileName = fileName
        self.__bufferNumber = bufferNumber
        # self.__lock=threading.Lock()
        self.__infoList = []
        self.__errList = []

    def Log(self, content, err=False, isPrint=False):
        text = str(datetime.now()) + ': ' + str(content)
        if (text != ""):
            if (isPrint):
                print(text)
            #            self.__lock.acquire()
            try:
                if err:
                    self.__errList.append(text)
                else:
                    self.__infoList.append(text)
            except Exception as e:
                raise e

    #            finally:
    #                self.__lock.release()

    def __WriteToFile(self, doNow=False):
        self.__lock.acquire()
        try:
            infoNum = len(self.__infoList)
            errNum = len(self.__errList)
            if (doNow and infoNum > 0):
                WriteLogToTxt(self.__infoList, False, self.__fileName, self.__logName)
                self.__infoList.clear()
            if (doNow and errNum > 0):
                WriteLogToTxt(self.__errList, True, self.__fileName, self.__logName)
                self.__errList.clear()
        except Exception as e:
            raise e
        finally:
            self.__lock.release()

    def Close(self):
        """
        最终将剩余日志写入文本，不然日志可能丢失
        """
        self.__WriteToFile(True)


def WriteLogToTxt(txtList, isError=False, folder='', fileName=''):
    """
    写文本函数
    如果isError=True自动添加'_error.txt'到文件末
    如果isError=False自动添加'.txt'到文件末
    路径名可以取名为'./log/'
    文件名可以取名为'Today'
    """
    try:
        if (os.path.exists(folder) == False):
            os.makedirs(folder)
        if (isError):
            with open(folder + fileName + '_error.txt', 'a', encoding='utf-8') as f:
                f.writelines(txtLine + '\n' for txtLine in txtList)
        else:
            with open(folder + fileName + '.txt', 'a', encoding='utf-8') as f:
                f.writelines(txtLine + '\n' for txtLine in txtList)
    except Exception as e:
        raise e
