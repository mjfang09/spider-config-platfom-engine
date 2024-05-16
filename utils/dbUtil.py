#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   dbUtil.py
@Time    :   2020/12/22 09:38:44
@Author  :   Jianhua Ye
@Phone    :   15655140926
@E-mail :   yejh@finchina.com
@License :   (C)Copyright Financial China Information & Technology Co., Ltd.
@Desc    :   Description
@Version :   1.0
'''
import abc

# here put the import lib
import pymssql
from dbutils.pooled_db import PooledDB

import utils.settings


class DataMapperInterface(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def execute_sql(self, sql, *args):
        pass

    @abc.abstractmethod
    def query(self, sql, param, *args):
        pass

    @abc.abstractmethod
    def to_dict(self, obj):
        pass


class OhMyDb(DataMapperInterface):
    def __init__(self, db_env='test'):
        conf = utils.settings.DB_INFO.get(db_env)
        self.__DBPool = PooledDB(
            creator=pymssql,
            mincached=utils.settings.MIN_CACHED,
            maxcached=utils.settings.MAX_CACHED,
            maxshared=utils.settings.MAX_SHARED,
            maxconnections=utils.settings.MAX_CONNECTIONS,
            blocking=utils.settings.BLOCKING,
            host=conf['DB_HOST'],
            user=conf['DB_USER'],
            password=conf['DB_PSWD'],
            database=conf['DB_BASE'],
            charset=conf['DB_CHAR']
        )

    def query(self, sql, to_dict=True):
        """
        :function: 查询SQL
        :param: sql-语句 to_dict:是否转字典
        :return: tuple/dict
        """
        conn = self.__DBPool.connection()
        cursor = conn.cursor(as_dict=to_dict)
        try:
            cursor.execute(sql)
            results = cursor.fetchall()
        except Exception as e:
            raise Exception(f'查询出错 - {sql} - {e}')
        else:
            return results
        finally:
            cursor.close()
            conn.close()

    def query_by_param(self, sql, param, to_dict=True):
        """
        :function: 参数查询SQL
        :param: sql-语句 param-参数元组 to_dict:是否转字典
        :return: tuple/dict
        """
        conn = self.__DBPool.connection()
        cursor = conn.cursor(as_dict=to_dict)
        cursor.execute(sql, param)
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return results

    def execute_sqls(self, sql_list):
        """
        :function: 批量执行SQL
        :param: sql_list-语句列表
        :return: boolean
        """
        conn = self.__DBPool.connection()
        cursor = conn.cursor()
        try:
            for sql in sql_list:
                cursor.execute(sql)
        except Exception as e:
            conn.rollback()  # 事务回滚
            raise Exception(f'SQL执行失败 - {sql} - {e}')
        else:
            conn.commit()  # 事务提交
            return True
        finally:
            cursor.close()
            conn.close()

    def execute_sqls_by_param(self, sql, params_list):
        """
        :function: 批量执行带参SQL
        :param: sql-语句 params_list-参数列表
        :return: boolean
        """
        conn = self.__DBPool.connection()
        cursor = conn.cursor()
        try:
            for params in params_list:
                cursor.execute(sql, tuple(params))
        except Exception as e:
            conn.rollback()
            raise Exception(f'SQL执行失败 - {sql} - {e}')
        else:
            conn.commit()
            return True
        finally:
            cursor.close()
            conn.close()

    def execute_sql(self, sql, *args):
        """
        :function: 执行单条SQL
        :param: sql-语句
        :return: boolean
        """
        if args:
            # TODO generate test
            return self.execute_sql_by_param(sql, *args)
        conn = self.__DBPool.connection()
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
        except Exception as e:
            conn.rollback()  # 事务回滚
            raise Exception(f'SQL执行失败 - {sql} - {e}')
        else:
            conn.commit()  # 事务提交
            return True
        finally:
            cursor.close()
            conn.close()

    def execute_sql_by_param(self, sql, param):
        """
        :function: 执行单条带参SQL
        :param: sql-语句 params_list-参数列表
        :return: boolean
        """
        conn = self.__DBPool.connection()
        cursor = conn.cursor()
        try:
            cursor.execute(sql, tuple(param))
        except Exception as e:
            conn.rollback()
            raise Exception(f'SQL执行失败 - {sql} - {e}')
        else:
            conn.commit()  # 事务提交
            return True
        finally:
            cursor.close()
            conn.close()

    def to_dict(self, obj):
        pass


def main():
    db_env = 'test'
    OPSQL = OhMyDb(db_env)


if __name__ == "__main__":
    main()
