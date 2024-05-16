#!/usr/bin/env python
# -*- encoding: utf-8 -*-
"""
@File    :   settings.py
@Time    :   2020/12/19 15:36:04
@Author  :   Jianhua Ye
@Phone    :   15655140926
@E-mail :   yejh@finchina.com
@License :   (C)Copyright Financial China Information & Technology Co., Ltd.
@Desc    :   配置文件信息
@Version :   1.0
"""

# 数据库环境
DB_INFO = {
    "test": {
        "DB_HOST": "10.17.207.185",
        "DB_USER": "dbinput",
        "DB_PSWD": "123456",
        "DB_BASE": "RREPORTTASK",
        "DB_CHAR": "UTF8",
    },
    "formal": {
        "DB_HOST": "10.17.205.164",
        "DB_USER": "apphct",
        "DB_PSWD": "apphct",
        "DB_BASE": "RREPORTTASK",
        "DB_CHAR": "UTF8",
    },
    "185": {
        "DB_HOST": "10.17.207.185",
        "DB_USER": "dbinput",
        "DB_PSWD": "123456",
        "DB_BASE": "RREPORTTASK",
        "DB_CHAR": "utf8",
    },
    "164": {
        "DB_HOST": "10.17.205.164",
        "DB_USER": "apphct",
        "DB_PSWD": "apphct",
        "DB_BASE": "RREPORTTASK",
        "DB_CHAR": "UTF8",
    }
}

# 消息队列环境
MQ_INFO = {
    "test": {
        "MQ_HOST": ['10.17.207.61', '10.17.207.78', '10.17.207.94'],
        "MQ_USER": "admin",
        "MQ_PSWD": "finchina",
        "MQ_PORT": "5672",
    },
    "formal": {
        "MQ_HOST": ['10.17.206.140', '10.17.205.97', '10.17.205.94'],
        "MQ_USER": "finchina",
        "MQ_PSWD": "finchina",
        "MQ_PORT": "5672",
    },
}

# 日志环境
LOGS_INFO = {
    "test": {
        "LOGS_DIR": "logs_test",
        "LOG_Level": "INFO",
        "CONSOLE_Level": "DEBUG",
    },
    "formal": {
        "LOGS_DIR": "logs_formal",
        "LOG_Level": "INFO",
        "CONSOLE_Level": "DEBUG",
    }
}

# 附件路径
FILE_INFO = {
    "test": "\\\\fileserver.finchina.local\\hct\\test",
    "formal": "\\\\fileserver.finchina.local\\hct",
}

# MongoDB环境
MONGO_INFO = {
    "test": {
        "CLIENT": "mongodb://10.17.205.83:27017/",
        "DB": "spider-lcjz",
        "COL_URL": "url_guid",
        "COL_DATA": "data_guid",
        "COL_DATA_NEW": "data_guid_new",
        "COL_DATA_MODULE_CT_CFPNAV": "CT_CFPNAV",
        "COL_DATA_MODULE_CT_CFPNAV_CUR": "CT_CFPNAV_CUR",
        "COL_DATA_MODULE_CT_PFPNAV": "CT_PFPNAV",
        "COL_DATA_MODULE_CT_PFPNav_CUR": "CT_PFPNav_CUR",
        "COL_DATA_MODULE_CT_Iperunit": "CT_Iperunit",
        "COL_DATA_MODULE_CT_IPerPrice": "CT_IPerPrice",
        "COL_DATA_MODULE_CT_InsRate": "CT_InsRate",
        "COL_DATA_MODULE_CT_PFNAV": "CT_PFNAV",
        "COL_DATA_MODULE_CT_PFNAV_CUR": "CT_PFNAV_CUR",
        "COL_DATA_MODULE_CT_NAV_All": "CT_NAV_All",
        "COL_DATA_MODULE_CT_NAV_CUR_All": "CT_NAV_CUR_All",
        "COL_DATA_MODULE_CT_TRUSTNAV": "CT_TRUSTNAV",
        "COL_DATA_MODULE_CT_TRUSTNAV_cur": "CT_TRUSTNAV_cur",
        "COL_DATA_MODULE_CT_CgRecJJYXH": "CT_CgRecJJYXH",
        "COL_DATA_MODULE_CT_PFPAnnounMT": "CT_PFPAnnounMT",
        "COL_DATA_MODULE_CT_CfpAnnounceMent": "CT_CfpAnnounceMent",
    },
    "formal": {
        "CLIENT": "mongodb://zqadmin:zqadmin@10.17.206.130:27017,10.17.206.131:27017,10.17.206.132:27017/",
        "DB": "spider-lcjz",
        "COL_URL": "url_guid",
        "COL_DATA": "data_guid",
        "COL_DATA_NEW": "data_guid_new",
        "COL_DATA_MODULE_CT_CFPNAV": "CT_CFPNAV",
        "COL_DATA_MODULE_CT_CFPNAV_CUR": "CT_CFPNAV_CUR",
        "COL_DATA_MODULE_CT_PFPNAV": "CT_PFPNAV",
        "COL_DATA_MODULE_CT_PFPNav_CUR": "CT_PFPNav_CUR",
        "COL_DATA_MODULE_CT_Iperunit": "CT_Iperunit",
        "COL_DATA_MODULE_CT_IPerPrice": "CT_IPerPrice",
        "COL_DATA_MODULE_CT_InsRate": "CT_InsRate",
        "COL_DATA_MODULE_CT_PFNAV": "CT_PFNAV",
        "COL_DATA_MODULE_CT_PFNAV_CUR": "CT_PFNAV_CUR",
        "COL_DATA_MODULE_CT_NAV_All": "CT_NAV_All",
        "COL_DATA_MODULE_CT_NAV_CUR_All": "CT_NAV_CUR_All",
        "COL_DATA_MODULE_CT_TRUSTNAV": "CT_TRUSTNAV",
        "COL_DATA_MODULE_CT_TRUSTNAV_cur": "CT_TRUSTNAV_cur",
        "COL_DATA_MODULE_CT_CgRecJJYXH": "CT_CgRecJJYXH",
        "COL_DATA_MODULE_CT_PFPAnnounMT": "CT_PFPAnnounMT",
        "COL_DATA_MODULE_CT_CfpAnnounceMent": "CT_CfpAnnounceMent",

    }
}

# 初始化连接池时创建的连接数
MIN_CACHED = 2

# 池中空闲连接的最大数量
MAX_CACHED = 5

# 池中共享连接的最大数量
MAX_SHARED = 3

# 被允许的最大连接数
MAX_CONNECTIONS = 6

# 是否可阻塞
BLOCKING = True
