#!/usr/bin/env python
# -*- coding: utf-8 -*-

import MySQLdb


class ProMySqlDB(object):
    """
    MySQL数据库类
    """

    def __init__(self, dbName, user, passwd, host='192.168.2.123', port=3306):
        try:
            self.dbName = str(dbName)
            self.user = str(user)
            self.passwd = str(passwd)
            self.host = str(host)
            self.port = int(port)
            self.__conn = MySQLdb.connect(user=self.user, db=self.dbName, passwd=self.passwd, host=self.host,
                                          charset='utf8', port=self.port)
        except MySQLdb.Error as e:
            raise e

    def Sql(self, sql):
        """
        执行SQL语句
        """
        try:
            c = self.__conn.cursor()
            c.execute(sql)
            self.__conn.commit()
        except Exception as e:
            self.__conn.rollback()
            raise e
        finally:
            c.close()

    def Sqls(self, sqls, values):
        """
        执行批量SQL语句
        """
        try:
            c = self.__conn.cursor()
            c.executemany(sqls, values)
            self.__conn.commit()
        except Exception as e:
            self.__conn.rollback()
            raise e
        finally:
            c.close()

    def GetResults(self, sql):
        """
        用于获取查询所有结果
        """
        try:
            c = self.__conn.cursor()
            c.execute(sql)
            results = c.fetchall()
            return results
        except Exception as e:
            raise e
        finally:
            c.close()

    def Close(self):
        """
        最终将数据库连接关闭
        """
        self.__conn.close()
