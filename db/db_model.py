#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Project ：  spider-nav-config-platform2
@File    :   db_model.py
@Time    :   2024/3/20 17:44
@Author  :   MengJia_Fang 501738
@E-mail  :   fangmj@finchina.com
@License :   (C)Copyright Financial China Information & Technology Co., Ltd.
@Desc    :   Description
@Version :   1.0
"""

import re


class Field:
    def __init__(self, name, cloumn_type, primary_key: bool = False):
        """

        :param name: 字段名称
        :param cloumn_type: sqlite支持 integer real text blob ；sqlserver支持...
        :param primary_key: 是否设置为主键
        """
        self.name = name
        self.column_type = cloumn_type
        self.primary_key = primary_key

    def __str__(self):
        return "< %s:%s >" % (self.__class__.__name__, self.name)


class StringField(Field):
    def __init__(self, name, primary_key: bool = False):
        super(StringField, self).__init__(name, 'varchar(1000)', primary_key=primary_key)


class IntegerField(Field):
    def __init__(self, name, primary_key: bool = False):
        super(IntegerField, self).__init__(name, 'int', primary_key=primary_key)


class FloatField(Field):
    def __init__(self, name, primary_key: bool = False):
        super(FloatField, self).__init__(name, 'float', primary_key=primary_key)


class ModelMetaClass(type):
    def __new__(cls, name, bases, attrs):
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        print(f'Start initializing model: {name}')
        mappings = dict()
        for k, v in attrs.items():
            if isinstance(v, Field):
                mappings[k] = v
        for k in mappings.keys():
            attrs.pop(k)

        attrs['__mappings__'] = mappings
        if attrs.get('table_name', None):
            if isinstance(attrs['table_name'], str):
                attrs['__table__'] = attrs['table_name']
            attrs.pop('table_name')
        else:
            attrs['__table__'] = name

        return type.__new__(cls, name, bases, attrs)


class Model(dict, metaclass=ModelMetaClass):
    """
    可通过添加类属性：table_name指定表名称
    """

    def __init__(self, sqlserver_db_driver_info='', sqlite_db_file_path='', mongo_db_conn_info='',
                 **kwargs):
        """

        :param sqlserver_db_driver_info: 'sqlserver::<server_ip>::<port>::<database_name>::<user>::<password>'
        :param sqlite_db_file_path: ./test.db
        :param kwargs:
        """
        # if not (sqlserver_db_driver_info or sqlite_db_file_path or mongo_db_conn_info):
        #     raise Exception('Provide at least one database information.')

        if sqlserver_db_driver_info:
            pass
        if sqlite_db_file_path:
            pass
        if mongo_db_conn_info:
            pass

        super(Model, self).__init__(**kwargs)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError("'Model' objects has no attribute '%s' " % key)

    def __setattr__(self, key, value):
        self[key] = value

    def destroy_db(self):
        """
        解决sqlite3报错：database is locked
            同一进程内的多个线程将不同的 sqlite3* 指针用于sqlite3_open()函数（即打开同一个数据库，但它们是不同的连接，因为sqlite3* 指针各自不同），并向同一数据库执行写入操作时会报错。
            每创建一个model实例就初始化了一个sqlite3* 指针

        多线程时每次操作数据库，需手动调用该方法
        """
        try:
            if self.get('__sqlserver_db', ''):
                self.__sqlserver_db.destroy()

            if self.get('__sqlite_db', ''):
                self.__sqlite_db.destroy()
        except Exception as e:
            print(e)

    def get_insert_sql_to_sqlserver(self):
        """
        插入数据时，当前model对象有的属性才会插入
        :return:
        """
        fields = []
        params = []
        args = []
        for k, v in self.__mappings__.items():
            arg_value = getattr(self, k, None)
            if arg_value is not None:
                fields.append(k)
                params.append('%s')
                args.append(arg_value)
        insert_sql = f"insert into {self.__table__}({','.join(fields)}) values ({','.join(params)})"
        return insert_sql, args

    def get_query_one_sql_to_sqlserver(self, query_params: list = [], query_sql: str = '',
                                       args: list = []):
        """
        :param args:搭配带有占位符的query_sql使用
        :param query_sql: 留给不想传参 想直接执行sql
        :param query_params: ['param_name1','param_name2',...]
        :return:
        """
        if not query_sql:
            # sql不存在时，传入的args参数无意义
            args = []
            query_sql = f"select * from {self.__table__}"
            if query_params and isinstance(query_params, list):
                query_sql += " where "
                for k in query_params:
                    value = getattr(self, k, None)
                    if value is not None:
                        query_sql += f"{k} = ? and "
                        args.append(value)
                    else:
                        query_sql += f"{k} is null and "
                query_sql = re.sub('( and )$', '', query_sql)
                query_sql = re.sub('( where )$', '', query_sql)
            elif not isinstance(query_params, list):
                raise Exception('query_params type must be list')

        return query_sql, args

    def get_query_all_sql_to_sqlserver(self, query_params: list = [], query_sql: str = '',
                                       args: list = []):
        '''

        :param args:
        :param query_sql:留给不想传参 想直接执行sql
        :param query_params: ['','','',]
        :return:
        '''

        if not query_sql:
            # sql为空时，传入的args没有意义
            args = []
            query_sql = f"select * from {self.__table__}"
            if query_params and isinstance(query_params, list):
                query_sql = " where "
                for k in query_params:
                    value = getattr(self, k, None)
                    if value is not None:
                        query_sql += f"{k} = ? and "
                        args.append(value)
                    else:
                        query_sql += f"{k} is null and "
                query_sql = re.sub('( and )$', '', query_sql)
                query_sql = re.sub('( where )$', '', query_sql)
            elif not isinstance(query_params, list):
                raise Exception('query_params type must be list')
        if args:
            result = query_sql, args
        else:
            result = query_sql
        return result

    def get_delete_sql_to_sqlserver(self, params: list = [], delete_sql: str = '', args: list = []):
        """

        :param args:
        :param params:
        :param delete_sql:
        :return:
        """
        if not delete_sql:
            # sql不存在时，传入的args参数无意义
            args = []
            delete_sql = f"delete from {self.__table__}"
            if params and isinstance(params, list):
                delete_sql += f" where "
                for k in params:
                    value = getattr(self, k, None)
                    if value is not None:
                        delete_sql += f"{k} = ? and "
                        args.append(value)
                    else:
                        delete_sql += f"{k} is null and "
                delete_sql = re.sub('( and )$', '', delete_sql)
                delete_sql = re.sub('( where )$', '', delete_sql)
            elif not isinstance(params, list):
                raise Exception('query_params type must be list')

        return delete_sql, args

    def get_update_sql_to_sqlserver(self, update_params: list = [], query_params: list = [],
                                    update_sql: str = '', args: list = []):
        """

        :param args:
        :param update_params:
        :param query_params:
        :param update_sql:
        :return:
        """
        if not update_sql:
            # sql 为空时，传入的args无意义
            args = []
            if isinstance(update_params, list) and isinstance(query_params, list):
                update_sql = f"update {self.__table__} set "
                for k in update_params:
                    value = getattr(self, k, None)
                    if value is not None:
                        update_sql += f"{k} = ? , "
                        args.append(value)
                if not args:
                    raise Exception('The current model does not contain field in update_params')
                update_sql = re.sub('( , )$', '', update_sql)
                update_sql += ' where '

                for q_k in query_params:
                    value = getattr(self, q_k, None)
                    if value is not None:
                        update_sql += f"{q_k} = ? and "
                        args.append(value)
                    else:
                        update_sql += f"{q_k} is null and "
                update_sql = re.sub('( and )$', '', update_sql)
                update_sql = re.sub('( where )$', '', update_sql)

            else:
                raise Exception("update_params and query_params type must be list")

        return update_sql, args

    def get_create_sqlite_tab_sql(self):
        # 'id integer primary key'
        fields = []
        primary_keys = []
        for k, v in self.__mappings__.items():
            if v.primary_key:
                primary_keys.append(k)

            if 'varchar' in v.column_type:
                k += ' text'
            elif 'int' in v.column_type:
                k += ' integer'
            elif 'float' in v.column_type:
                k += ' real'
            else:
                raise KeyError(
                    'sqlite3 Unsupport column type: %s,column name: %s' % (v.column_type, k))

            fields.append(k)

        if primary_keys:
            primary_key_str = 'primary key (' + ','.join(primary_keys) + ')'
        else:
            primary_key_str = '_id_ integer primary key'
        fields.append(primary_key_str)

        c_tab_sql = f"CREATE TABLE IF NOT EXISTS {self.__table__} ({','.join(fields)})"
        return c_tab_sql

    # def save_of_sqlite(self):
    #     fields = []
    #     params = []
    #     args = []
    #     for k, v in self.__mappings__.items():
    #         arg_value = getattr(self, k, None)
    #         if arg_value is not None:
    #             fields.append(k)
    #             params.append('?')
    #             args.append(arg_value)
    #     insert_sql = f"insert into {self.__table__}({','.join(fields)}) values ({','.join(params)})"
    #     return self.__sqlite_db.insert_one(insert_sql, args)
    #
    # def get_one_of_sqlite(self, query_params: list = None):
    #     '''
    #
    #     :param query_params: ['','','',]
    #     :return: True:existed False:does not existed
    #     '''
    #     query_sql = f"select * from {self.__table__}"
    #     args = []
    #     if query_params and isinstance(query_params, list):
    #         query_sql += " where "
    #         for k in query_params:
    #             value = getattr(self, k, None)
    #             if value is not None:
    #                 query_sql += f"{k} = ? and "
    #                 args.append(value)
    #             else:
    #                 query_sql += f'{k} is null and '
    #         query_sql = re.sub('( and )$', '', query_sql)
    #         query_sql = re.sub('( where )$', '', query_sql)
    #     elif not isinstance(query_params, list):
    #         raise Exception("query_params type must be list")
    #     # logger.info(query_sql)
    #     return self.__sqlite_db.fetchone(query_sql, args)
    #
    # def delete_by_param_of_sqlite(self, params: list = None):
    #     """
    #
    #     :param params: ['','','',...]
    #     :return:
    #     """
    #     delete_sql = f"delete from {self.__table__}"
    #     args = []
    #     if params and isinstance(params, list):
    #         delete_sql += " where "
    #         for k in params:
    #             value = getattr(self, k, None)
    #             if value is not None:
    #                 delete_sql += f"{k} = ? and "
    #                 args.append(value)
    #             else:
    #                 delete_sql += f"{k} is null and "
    #         delete_sql = re.sub('( and )$', '', delete_sql)
    #         delete_sql = re.sub('( where )$', '', delete_sql)
    #     elif not isinstance(params, list):
    #         raise Exception("params type must be list")
    #
    #     return self.__sqlite_db.execute(delete_sql, args)
    #
    # def update_of_sqlite(self, update_params: list, query_params: list):
    #     """
    #
    #     :param update_params: ['','',...]
    #     :param query_params: ['','',...]
    #     :return:
    #     """
    #     if isinstance(update_params, list) and isinstance(query_params, list):
    #         update_sql = f"update {self.__table__} set "
    #         args = []
    #         for k in update_params:
    #             u_value = getattr(self, k, None)
    #             if u_value is not None:
    #                 update_sql += f"{k} = ? , "
    #                 args.append(u_value)
    #         if not args:
    #             raise Exception('The current model does not contain field in update_params')
    #         update_sql = re.sub('( , )$', '', update_sql)
    #         update_sql += ' where '
    #
    #         for q_k in query_params:
    #             q_value = getattr(self, q_k, None)
    #             if q_value is not None:
    #                 update_sql += f"{q_k} = ? and "
    #                 args.append(q_value)
    #             else:
    #                 update_sql += f"{q_k} is null and "
    #         update_sql = re.sub('( and )$', '', update_sql)
    #         update_sql = re.sub('( where )$', '', update_sql)
    #
    #     else:
    #         raise Exception("update_params and query_params type must be list")
    #
    #     return self.__sqlite_db.update(update_sql, args)
    #
    # def find_one_of_mongo(self, param):
    #     m_obj = self.__mongo_db.collection.find_one(param)
    #     return m_obj
    #
    # def delete_one_of_mongo(self, param):
    #     m_obj = self.__mongo_db.collection.delete_one(param)
    #     return m_obj
    #
    # def insert_one_of_mongo(self, param):
    #     m_obj = self.__mongo_db.collection.insert_one(param)
    #     return m_obj


class CTNAVLISTURLModel(Model):
    table_name = 'ct_nav_list_url'

    # 配置信源编码
    sourceid = StringField('sourceid')

    # 业务信源编码
    grabdatacode = StringField('grabdatacode')

    # 网址
    url = StringField('url')

    # # post请求参数
    # postdata = StringField('postdata')

    # # 返回体是否是json
    # isjson = IntegerField('isjson')
    #
    # header = StringField('header')

    # # 正文是否为空
    # empty_content = IntegerField('empty_content')
    #
    # # 是否正文去重
    # duplicate_content = IntegerField('duplicate_content')

    # # 编码格式
    # textencoding = StringField('textencoding')
    #
    # # 数据解析规则
    # field_rule = StringField('field_rule')

    # # 是否使用代理
    # useproxy = IntegerField('useproxy')
    #
    # # 代理
    # proxys = StringField('proxys')
    #
    # # 插件
    # plugins = StringField('plugins')

    # 网址唯一编码
    guid = StringField('guid')

    # 抓取状态
    state = StringField('state')

    # 所放置队列名称
    queuename = StringField('queuename')

    # 备注
    memo = StringField('memo')


class CTNAVTRANSITDATAModel(Model):
    table_name = 'CT_NAV_TRANSIT_DATA'
    # 网站
    sitename = StringField('sitename')

    # 目录
    sitesort = StringField('sitesort')

    # 信源配置编码
    sourceid = StringField('sourceid')

    # 信源编码
    grabdatacode = StringField('grabdatacode')

    # 数据的唯一编码
    guid = StringField('guid')

    list_mid_data = StringField('list_mid_data')

    # # 网址
    # url = StringField('url')

    # # 净值类型
    # modulecode = IntegerField('modulecode')

    # # 是否是参考公募
    # is_ckgm = IntegerField('is_ckgm')

    # # 对应list_url网址表中的guid
    # parent_guid = StringField('parent_guid')
    #
    # # 公告日期
    # declaredate = StringField('declaredate')
    #
    # # 数据更新日期
    # updatedate = StringField('updatedate')
    #
    # # 起始日期-集合货币净值
    # begindate = StringField('begindate')
    #
    # # 产品名称
    # prodname = StringField('prodname')
    #
    # # 产品代码
    # prodCode = StringField('prodCode')
    #
    # # 产品编号
    # prodnumber = StringField('prodnumber')
    #
    # # 净值日期
    # navdate = StringField('navdate')
    #
    # # 币种-净值
    # nav_currency = StringField('nav_currency')
    #
    # # 净值-费用扣除
    # nav_flag1 = StringField('nav_flag1')
    #
    # # 净值-单位净值
    # nav1 = StringField('nav1')
    #
    # # 净值-累计净值
    # nav2 = StringField('nav2')
    #
    # # 净值-累计收益率
    # nav3 = StringField('nav3')
    #
    # # 净值-年化收益率
    # nav4 = StringField('nav4')
    #
    # # 净值-累计年化收益率
    # nav5 = StringField('nav5')

    # # 净值-原文地址-------是否还要保留
    # textlink = StringField('textlink')

    # # 货币-特殊标志
    # nav_cur_flag1 = StringField('nav_cur_flag1')
    #
    # # 货币-每万份计划收益
    # nav_cur1 = StringField('nav_cur1')
    #
    # # 货币-七日年化收益率
    # nav_cur2 = StringField('nav_cur2')
    #
    # # 货币-年化收益率
    # nav_cur3 = StringField('nav_cur3')

    # 系统编码
    systemno = StringField('systemno')

    # 状态
    state = IntegerField('state')

    # 备注
    memo = StringField('memo')


class CTCFPNAVModel(Model):
    table_name = 'CT_CFPNAV'

    guid = StringField('guid')

    # # 中转表id
    # parentguid = StringField('parentguid')

    sourceid = StringField('sourceid')

    grabdatacode = StringField('grabdatacode')

    # 产品简称
    ptsname = StringField('ptsname')

    # 公告日期
    declaredate = StringField('declaredate')

    # 资料更新日期
    updatedate = StringField('updatedate')

    # 变动日期
    publishdate = StringField('publishdate')

    # 单位净值
    cfpnav1 = StringField('cfpnav1')

    # 累计净值
    cfpnav2 = StringField('cfpnav2')

    # 币种
    currency = StringField('currency')

    # 编码
    systemno = StringField('systemno')

    # 网站
    sitename = StringField('sitename')

    # 目录一
    sitesort1 = StringField('sitesort1')

    # 目录二
    sitesort2 = StringField('sitesort2')

    # 网址
    url = StringField('url')

    # 状态
    state = IntegerField('state')

    memo = StringField('memo')


class CTCFPNAVCURModel(Model):
    table_name = 'CT_CFPNAV_CUR'

    guid = StringField('guid')

    # # 中转表id
    # parentguid = StringField('parentguid')

    sourceid = StringField('sourceid')

    grabdatacode = StringField('grabdatacode')

    # 产品简称
    ptsname = StringField('ptsname')

    # 公告日期
    declaredate = StringField('declaredate')

    # 资料更新日期
    updatedate = StringField('updatedate')

    # 起始日期
    begindate = StringField('begindate')

    # 截止日期
    enddate = StringField('enddate')

    # 标志
    cfpnav_cur1 = StringField('cfpnav_cur1')

    # 每万份计划收益
    cfpnav_cur2 = StringField('cfpnav_cur2')

    # 七日年化收益率
    cfpnav_cur3 = StringField('cfpnav_cur3')

    # 年化收益率
    cfpnav_cur4 = StringField('cfpnav_cur4')

    # 编码
    systemno = StringField('systemno')

    # 网站
    sitename = StringField('sitename')

    # 目录一
    sitesort1 = StringField('sitesort1')

    # 目录二
    sitesort2 = StringField('sitesort2')

    # 网址
    url = StringField('url')

    # 状态
    state = IntegerField('state')

    # 备注
    memo = StringField('memo')


class CTPFPNAVModel(Model):
    table_name = 'CT_PFPNAV'
    guid = StringField('guid')

    # # 中转表id
    # parentguid = StringField('parentguid')

    sourceid = StringField('sourceid')

    grabdatacode = StringField('grabdatacode')

    # 网站
    sitename = StringField('sitename')

    # 目录一
    sitesort1 = StringField('sitesort1')

    # 目录二
    sitesort2 = StringField('sitesort2')

    # 网址
    url = StringField('url')

    # 产品全称
    ptsname = StringField('ptsname')

    # 公告日期
    declaredate = StringField('declaredate')

    # 产品代码
    pfproductcode = StringField('pfproductcode')

    # 截至日期
    pfpnav1 = StringField('pfpnav1')

    # 币种
    pfpnav2 = StringField('pfpnav2')

    # 单位净值
    pfpnav3 = StringField('pfpnav3')

    # 累计净值
    pfpnav4 = StringField('pfpnav4')

    # 累计收益率
    pfpnav5 = StringField('pfpnav5')

    # 年化收益率
    pfpnav6 = StringField('pfpnav6')

    # 累计年化收益率
    pfpnav7 = StringField('pfpnav7')

    # 费用扣除
    pfpnav8 = StringField('pfpnav8')

    # 产品编码
    pfpnav9 = StringField('pfpnav9')

    # 备注
    memo = StringField('memo')

    # 系统编码
    systemno = StringField('systemno')

    # # 原文地址
    # textlink = StringField('textlink')

    state = IntegerField('state')


class CTPFPNavCURModel(Model):
    table_name = 'CT_PFPNav_CUR'

    guid = StringField('guid')

    # # 中转表id
    # parentguid = StringField('parentguid')

    sourceid = StringField('sourceid')

    grabdatacode = StringField('grabdatacode')

    declaredate = StringField('declaredate')

    # 资料更新日期
    updatedate = StringField('updatedate')

    # 产品代码
    pfproductCode = StringField('pfproductCode')

    # 截止日期
    enddate = StringField('enddate')
    # 特殊标志
    pfpnav_cur1 = StringField('pfpnav_cur1')

    # 每万份计划收益
    pfpnav_cur2 = StringField('pfpnav_cur2')

    # 七日年化收益率
    pfpnav_cur3 = StringField('pfpnav_cur3')

    # 预估年化收益率
    pfpnav_cur4 = StringField('pfpnav_cur4')

    # 产品编号
    pfpnav_cur5 = StringField('pfpnav_cur5')

    # 编码
    systemno = StringField('systemno')

    # 网站
    sitename = StringField('sitename')

    # 目录一
    sitesort1 = StringField('sitesort1')

    # 目录二
    sitesort2 = StringField('sitesort2')

    # 备注
    memo = StringField('memo')

    # 网址
    url = StringField('url')

    # 产品简称
    pfproductname = StringField('pfproductname')

    state = IntegerField('state')


class CTIperunitModel(Model):
    # 保险价格
    table_name = 'CT_Iperunit'

    guid = StringField('guid')

    # # 中转表id
    # parentguid = StringField('parentguid')

    sourceid = StringField('sourceid')

    grabdatacode = StringField('grabdatacode')

    # 网址
    url = StringField('url')

    # 网站
    sitename = StringField('sitename')

    # 目录一
    sitesort1 = StringField('sitesort1')

    # 目录二
    sitesort2 = StringField('sitesort2')

    # 产品名称
    ptsname = StringField('ptsname')

    # 公告日期
    declaredate = StringField('declaredate')

    # 评估日期
    iperunit1 = StringField('iperunit1')

    # 买入价
    iperunit2 = StringField('iperunit2')

    # 卖出价
    iperunit3 = StringField('iperunit3')

    # 累计净值
    iperunit4 = StringField('iperunit4')

    # 系统编码
    systemno = StringField('systemno')

    state = IntegerField('state')

    # # 原文地址
    # textlink = StringField('textlink')


class CTIPerPriceModel(Model):
    # 保险货币型价格
    table_name = 'CT_IPerPrice'

    guid = StringField('guid')

    sourceid = StringField('sourceid')

    grabdatacode = StringField('grabdatacode')

    # 网址
    url = StringField('url')

    # 网站
    sitename = StringField('sitename')

    # 目录一
    sitesort1 = StringField('sitesort1')

    # 目录二
    sitesort2 = StringField('sitesort2')

    # 投资账户简称
    insperaname = StringField('insperaname')

    # 公告日期
    declaredate = StringField('declaredate')

    #
    # iperprice1 = StringField('iperprice1')

    # 截止日期
    iperprice2 = StringField('iperprice2')

    # 七日年化
    iperprice3 = StringField('iperprice3')

    # 万份收益
    iperprice4 = StringField('iperprice4')

    #
    # iperprice5 = StringField('iperprice5')

    # 备注
    memo = StringField('memo')

    # 系统编码
    systemno = StringField('systemno')

    state = IntegerField('state')

    # # 原文地址
    # textlink = StringField('textlink')


class CTInsRateModel(Model):
    # 保险万能利率
    table_name = 'CT_InsRate'

    guid = StringField('guid')

    sourceid = StringField('sourceid')

    grabdatacode = StringField('grabdatacode')

    # 网址
    url = StringField('url')

    # 网站
    sitename = StringField('sitename')

    # 目录一
    sitesort1 = StringField('sitesort1')

    # 目录二
    sitesort2 = StringField('sitesort2')

    # 公告日期
    declaredate = StringField('declaredate')

    # 更新日期
    updatedate = StringField('updatedate')

    # 账户名称
    insname = StringField('insname')

    # 结算期间
    insrate1 = StringField('insrate1')

    # 起始日期
    begindate = StringField('begindate')

    # 截止日期
    enddate = StringField('enddate')

    # 结算日利率
    insrate2 = StringField('insrate2')

    # 结算月利率
    insrate3 = StringField('insrate3')

    # 结算年利率
    insrate4 = StringField('insrate4')

    # 保底日利率
    insrate5 = StringField('insrate5')

    # 保底月利率
    insrate6 = StringField('insrate6')

    # 保底年利率
    insrate7 = StringField('insrate7')

    # 保单适用范围
    insrate8 = StringField('insrate8')

    # 系统编码
    systemno = StringField('systemno')

    state = IntegerField('state')


class CTPFNAVModel(Model):
    # 养老金净值
    table_name = 'CT_PFNAV'

    guid = StringField('guid')

    sourceid = StringField('sourceid')

    grabdatacode = StringField('grabdatacode')

    # 网址
    url = StringField('url')

    # 网站
    sitename = StringField('sitename')

    # 目录一
    sitesort1 = StringField('sitesort1')

    # 目录二
    sitesort2 = StringField('sitesort2')

    # 首次入库日期
    fwdate = StringField('fwdate')

    # 更新日期
    updatedate = StringField('updatedate')

    # 产品简称
    pfaname = StringField('pfaname')

    # 产品代码
    pfcode = StringField('pfcode')

    # 变动日期
    publishdate = StringField('publishdate')

    # 单位净值
    pfnav1 = StringField('pfnav1')

    # 结算日利率
    pfnav2 = StringField('pfnav2')

    # 累计净值
    insrate3 = StringField('insrate3')

    # 系统编码
    systemno = StringField('systemno')

    state = IntegerField('state')


class CTPFNAVCURModel(Model):
    # 养老金货币式净值
    table_name = 'CT_PFNAV_CUR'

    guid = StringField('guid')

    sourceid = StringField('sourceid')

    grabdatacode = StringField('grabdatacode')

    # 网址
    url = StringField('url')

    # 网站
    sitename = StringField('sitename')

    # 目录一
    sitesort1 = StringField('sitesort1')

    # 目录二
    sitesort2 = StringField('sitesort2')

    # 首次入库日期
    fwdate = StringField('fwdate')

    # 更新日期
    updatedate = StringField('updatedate')

    # 产品简称
    pfaname = StringField('pfaname')

    # 产品代码
    pfcode = StringField('pfcode')

    # 截止日期
    enddate = StringField('enddate')

    # 万份收益
    pfnav_cur2 = StringField('pfnav_cur2')

    # 年化收益率
    pfnav_cur3 = StringField('pfnav_cur3')

    # 系统编码
    systemno = StringField('systemno')

    state = IntegerField('state')


class CTNAVAllModel(Model):
    # 开放式基金净值
    table_name = 'CT_NAV_All'

    guid = StringField('guid')

    sourceid = StringField('sourceid')

    grabdatacode = StringField('grabdatacode')

    # 网址
    url = StringField('url')

    # 网站
    sitename = StringField('sitename')

    # 目录一
    sitesort1 = StringField('sitesort1')

    # 目录二
    sitesort2 = StringField('sitesort2')

    # 基金代码
    symbol = StringField('symbol')

    # 网页简称
    webname = StringField('webname')

    # 变动日期
    publishdate = StringField('publishdate')

    # 单位净值
    nav1 = StringField('nav1')

    # 累计净值
    nav2 = StringField('nav2')

    # 数据来源
    datasource = StringField('datasource')

    # 是否自动入库
    isautomatic = IntegerField('isautomatic')

    # 系统编码
    systemno = StringField('systemno')

    state = IntegerField('state')


class CTNAVCURAllModel(Model):
    # 货币式基金净值
    table_name = 'CT_NAV_CUR_All'

    guid = StringField('guid')

    sourceid = StringField('sourceid')

    grabdatacode = StringField('grabdatacode')

    # 网址
    url = StringField('url')

    # 网站
    sitename = StringField('sitename')

    # 目录一
    sitesort1 = StringField('sitesort1')

    # 目录二
    sitesort2 = StringField('sitesort2')

    # 基金代码
    symbol = StringField('symbol')

    # 网页简称
    webname = StringField('webname')

    # 变动日期
    publishdate = StringField('publishdate')

    # 特殊标志
    nav_cur1 = StringField('nav_cur1')

    # 公布份额收益
    nav_cur4 = StringField('nav_cur4')

    # 7日年化收益率
    nav_cur5 = StringField('nav_cur5')

    # 数据来源
    datasource = StringField('datasource')

    # 是否自动入库
    isautomatic = IntegerField('isautomatic')

    # 系统编码
    systemno = StringField('systemno')

    state = IntegerField('state')


class CTTRUSTNAVModel(Model):
    # 信托净值
    table_name = 'CT_TRUSTNAV'

    guid = StringField('guid')

    sourceid = StringField('sourceid')

    grabdatacode = StringField('grabdatacode')

    # 网址
    sourceurl = StringField('sourceurl')

    # 网站
    sitename = StringField('sitename')

    # 目录一
    sitesort1 = StringField('sitesort1')

    # 目录二
    sitesort2 = StringField('sitesort2')

    # 产品简称
    trustaname = StringField('trustaname')

    # 公告日期
    declaredate = StringField('declaredate')

    # 更新日期
    updatedate = StringField('updatedate')

    # 截止日期
    publishdate = StringField('publishdate')

    # 币种
    trustnav1 = StringField('trustnav1')

    # 单位净值
    trustnav4 = StringField('trustnav4')

    # 单位累计净值
    trustnav5 = StringField('trustnav5')

    # 累计收益率
    trustnav6 = StringField('trustnav6')

    # 备注
    memo = StringField('memo')

    # 系统编码
    systemno = StringField('systemno')

    state = IntegerField('state')


class CTTRUSTNAVCurModel(Model):
    # 信托货币式净值
    table_name = 'CT_TRUSTNAV_cur'

    guid = StringField('guid')

    sourceid = StringField('sourceid')

    grabdatacode = StringField('grabdatacode')

    # 网址
    url = StringField('url')

    # 网站
    sitename = StringField('sitename')

    # 目录一
    sitesort1 = StringField('sitesort1')

    # 目录二
    sitesort2 = StringField('sitesort2')

    # 产品简称
    trustaname = StringField('trustaname')

    # 公告日期
    declaredate = StringField('declaredate')

    # 更新日期
    updatedate = StringField('updatedate')

    # 截止日期
    enddate = StringField('enddate')

    # 每万份信托单位收益
    trustnav_cur2 = StringField('trustnav_cur2')

    # 七日年化收益率
    trustnav_cur3 = StringField('trustnav_cur3')

    # 三十日日年化收益率
    trustnav_cur4 = StringField('trustnav_cur4')

    # 系统编码
    systemno = StringField('systemno')

    state = IntegerField('state')


class CTCgRecJJYXHModel(Model):
    # 基金业协会违规公告
    table_name = 'CT_CgRecJJYXH'

    guid = StringField('guid')

    sourceid = StringField('sourceid')

    grabdatacode = StringField('grabdatacode')

    # 网址
    url = StringField('url')

    # 网站
    sitename = StringField('sitename')

    # 网站栏目1
    sitesort1 = StringField('sitesort1')

    # 网站栏目2
    sitesort2 = StringField('sitesort2')

    # 发布日期
    publishdate = StringField('publishdate')

    # 更新日期
    updatedate = StringField('updatedate')

    # 采集记录1
    cgrec1 = StringField('cgrec1')

    # 文件路径
    filepath = StringField('filepath')

    # 系统编码
    systemno = StringField('systemno')

    state = IntegerField('state')


class CTPFPAnnounMTModel(Model):
    # 理财公告
    table_name = 'CT_PFPAnnounMT'

    guid = StringField('guid')

    sourceid = StringField('sourceid')

    grabdatacode = StringField('grabdatacode')

    # 网站
    sitename = StringField('sitename')

    # 目录一
    sitesort1 = StringField('sitesort1')

    # 目录二
    sitesort2 = StringField('sitesort1')

    # 链接
    url = StringField('url')

    # 链接
    title = StringField('title')

    # 日期
    declaredate = StringField('declaredate')

    # 唯一编码
    uniquecode = StringField('uniquecode')

    # 内容
    content = StringField('content')

    # 编码
    systemno = StringField('systemno')

    state = IntegerField('state')

    memo = IntegerField('memo')


class CTPFPAnnounMTFILEModel(Model):
    # 理财公告附件表
    table_name = 'CT_PFPAnnounMT_FILE'

    parentguid = StringField('parentguid')

    spiderguid = StringField('spiderguid')

    sourceid = StringField('sourceid')

    grabdatacode = StringField('grabdatacode')

    # 附件目录
    downfilelist = StringField('downfilelist')

    # 附件名称
    downfilename = StringField('downfilename')

    # 附件链接
    downfileurl = StringField('downfileurl')

    # 唯一编码
    uniquecode = StringField('uniquecode')

    state = IntegerField('state')


class CTCfpAnnounceMentModel(Model):
    # 集合理财公告
    table_name = 'CT_CfpAnnounceMent'

    guid = StringField('guid')

    sourceid = StringField('sourceid')

    grabdatacode = StringField('grabdatacode')

    # 网站
    sitename = StringField('sitename')

    # 目录一
    sitesort1 = StringField('sitesort1')

    # 目录二
    sitesort2 = StringField('sitesort1')

    # 链接
    url = StringField('url')

    # 链接
    title = StringField('title')

    # 日期
    declaredate = StringField('declaredate')

    # 唯一编码
    uniquecode = StringField('uniquecode')

    # 内容
    content = StringField('content')

    # 编码
    systemno = StringField('systemno')

    state = IntegerField('state')

    memo1 = IntegerField('memo')


class CTCfpAnnounceMentFileModel(Model):
    # 集合理财公告附件表
    table_name = 'CT_CfpAnnounceMent_File'

    parentguid = StringField('parentguid')

    spiderguid = StringField('spiderguid')

    sourceid = StringField('sourceid')

    grabdatacode = StringField('grabdatacode')

    # 附件目录
    downfilelist = StringField('downfilelist')

    # 附件名称
    downfilename = StringField('downfilename')

    # 附件链接
    downfileurl = StringField('downfileurl')

    # 唯一编码
    uniquecode = StringField('uniquecode')

    state = IntegerField('state')
