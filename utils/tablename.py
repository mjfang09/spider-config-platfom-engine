#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Project ：  spider-nav-config-platform2
@File    :   __init__.py.py
@Time    :   2024/3/20 17:44
@Author  :   MengJia_Fang 501738
@E-mail  :   fangmj@finchina.com
@License :   (C)Copyright Financial China Information & Technology Co., Ltd.
@Desc    :   Description
@Version :   1.0
"""


# from enum import Enum


# class DownloadTableName(Enum):
#     CT_CFPNAV = 1  # 集合理财净值
#     CT_CFPNAV_CUR = 2  # 集合理财货币净值
#     CT_PFPNAV = 3  # 银行理财净值
#     CT_PFPNav_CUR = 4  # 银行理财货币净值
#     CT_Iperunit = 5  # 保险价格
#     CT_IPerPrice = 6  # 保险货币型价格
#     CT_InsRate = 7  # 保险万能利率
#     CT_PFNAV = 8  # 养老金净值
#     CT_PFNAV_CUR = 9  # 养老金货币式净值
#     CT_NAV_All = 10  # 开放式基金净值
#     CT_NAV_CUR_All = 11  # 货币式基金净值
#     CT_TRUSTNAV = 12  # 信托净值
#     CT_TRUSTNAV_cur = 13  # 信托货币式净值
#     CT_CgRecJJYXH = 14  # 基金业协会违规公告
#     CT_PFPAnnounMT = 15  # 理财公告,还有个附件表


def get_download_table(c) -> str:
    if c == 1:
        return "CT_CFPNAV"  # 集合理财净值
    elif c == 2:
        return "CT_CFPNAV_CUR"  # 集合理财货币净值
    elif c == 3:
        return "CT_PFPNAV"  # 银行理财净值
    elif c == 4:
        return "CT_PFPNav_CUR"  # 银行理财货币净值
    elif c == 5:
        return "CT_Iperunit"  # 保险价格
    elif c == 6:
        return "CT_IPerPrice"  # 保险货币型价格
    elif c == 7:
        return "CT_InsRate"  # 保险万能利率
    elif c == 8:
        return "CT_PFNAV"  # 养老金净值
    elif c == 9:
        return "CT_PFNAV_CUR"  # 养老金货币式净值
    elif c == 10:
        return "CT_NAV_All"  # 开放式基金净值
    elif c == 11:
        return "CT_NAV_CUR_All"  # 货币式基金净值
    elif c == 12:
        return "CT_TRUSTNAV"  # 信托净值
    elif c == 13:
        return "CT_TRUSTNAV_cur"  # 信托货币式净值
    elif c == 14:
        return "CT_CgRecJJYXH"  # 基金业协会违规公告
    elif c == 15:
        return "CT_PFPAnnounMT,CT_PFPAnnounMT_File"  # 银行公告，多张下载表时，第一张用作主表，对应mongodb去重表表名
    elif c == 16:
        return "CT_CfpAnnounceMent,CT_CfpAnnounceMent_File"  # 集合理财公告，多张下载表时，第一张用作主表，对应mongodb去重表表名


def get_module_zh_name(c) -> str:
    if c == 1:
        return '集合理财净值'
    elif c == 2:
        return '集合理财货币净值'
    elif c == 3:
        return '银行理财净值'
    elif c == 4:
        return '银行理财货币净值'
    elif c == 5:
        return '保险价格'
    elif c == 6:
        return '保险货币型价格'
    elif c == 7:
        return '保险万能利率'
    elif c == 8:
        return '养老金净值'
    elif c == 9:
        return '养老金货币式净值'
    elif c == 10:
        return '开放式基金净值'
    elif c == 11:
        return '货币式基金净值'
    elif c == 12:
        return '信托净值'
    elif c == 13:
        return '信托货币式净值'
    elif c == 14:
        return '基金业协会违规公告'
    elif c == 15:
        return '银行公告'
    elif c == 16:
        return '集合理财公告'
    else:
        raise Exception('不支持的模块编码')
