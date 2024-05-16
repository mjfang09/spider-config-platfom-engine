#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Project ：  spider-nav-config-platform 
@File    :   data_stored_threads.py
@Time    :   2024/3/20 13:44
@Author  :   MengJia_Fang 501738
@E-mail  :   fangmj@finchina.com
@License :   (C)Copyright Financial China Information & Technology Co., Ltd.
@Desc    :   只负责入库下载表
@Version :   1.0
"""
import datetime
import json
import os
import sys
import traceback
import pymongo

parent_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(parent_path)
from spider_logging.kafka_logging import start_hear_beat, listener
from spider_logging.kafkalogger import KafkaLogger
import utils.env
import utils.settings
from db.db_model import CTCFPNAVModel, CTCFPNAVCURModel, CTPFPNAVModel, CTPFPNavCURModel, \
    CTIperunitModel, CTIPerPriceModel, CTInsRateModel, CTPFNAVModel, CTPFNAVCURModel, CTNAVAllModel, \
    CTNAVCURAllModel, CTTRUSTNAVModel, CTTRUSTNAVCurModel, CTCgRecJJYXHModel, CTPFPAnnounMTModel, \
    CTPFPAnnounMTFILEModel, CTCfpAnnounceMentModel, CTCfpAnnounceMentFileModel
from utils.dbUtil import OhMyDb
from utils.mqUtil import RabbitMqManager
from utils.spider_constant import TEST_LC_JZ_DATA_TO_BE_STORED_QUEUE_NAME, \
    LC_JZ_DATA_TO_BE_STORED_QUEUE_NAME, DATA_STORE_FLAG
from utils.tablename import get_download_table
from utils.tools import OhMyExtractor

LOG_APP_ID = '55c545bb1d99451fab2f9e8bb9eba250'
LOGGER_NAME = 'spider.data_stored'
LOGGER = KafkaLogger(name=LOGGER_NAME, appid=LOG_APP_ID)
start_hear_beat(print_console=True, app_id=LOG_APP_ID)

# 数据库环境 test/formal
db_env = utils.env.DB_ENV
# 消息队列环境 test/formal
mq_env = utils.env.MQ_ENV

# mongodb环境 test/formal
mongo_env = utils.env.MONGO_ENV

OPSQL = OhMyDb(db_env)

MONGO = pymongo.MongoClient(utils.settings.MONGO_INFO[mongo_env]['CLIENT'])[
    utils.settings.MONGO_INFO[mongo_env]['DB']][
    utils.settings.MONGO_INFO[mongo_env]['COL_DATA_NEW']]
# path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
# sys.path.append(path)

plugin_path = os.path.join(
    os.path.abspath(os.path.dirname(os.path.abspath(__file__)) + os.path.sep), 'plugins')
sys.path.append(plugin_path)
MQ_Manager = ''


class DataStored:
    def __init__(self, extractor, mongo, pymssql):
        self.OhMyExtractor = extractor
        self.MONGO = mongo
        self.PY_MSSQL = pymssql

    def run(self, record):
        # thread_logger = KafkaLogger(name=LOGGER_NAME, appid=LOG_APP_ID)
        if not record.get('grabdatacode', '') or record['grabdatacode'] == 'None':
            LOGGER.set_data_source_id('0', durable=True)
        else:
            LOGGER.set_data_source_id(record['grabdatacode'], durable=True)

        originTime1 = datetime.datetime.now().strftime("%Y-%m-%d")
        originTime2 = datetime.datetime.now().strftime("%H:%M:%S")

        LOGGER.set_origin_msg_time(f'{originTime1} {originTime2}.000')
        sourceid = record['sourceid']
        stored_field = record['stored_fields']
        data_guid = record.get('data_guid', '')
        sub_data_guid = record.get('sub_data_guid', '')
        if not data_guid:
            LOGGER.insert_db.warning(
                f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {stored_field} - 入下载表报错 - 数据guid不存在!')
            raise Exception(
                f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {stored_field} - 入下载表报错 - 数据guid不存在!')

        if not record.get('modulecode', ''):
            LOGGER.insert_db.warning(
                f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {stored_field} - 入下载表报错 - modulecode字段不存在，无法获取对应的下载表名称!')
            raise Exception(
                f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {stored_field} - 入下载表报错 - modulecode字段不存在，无法获取对应的下载表名称!')
        # 直接入库
        download_table_name = get_download_table(record['modulecode'])
        if ',' in download_table_name:
            download_table_name = download_table_name.split(',')
        # 下载表
        db_model = self.get_download_db_model(
            download_table_name,
            stored_field,
            sourceid,
            record['grabdatacode'],
            data_guid,
            sub_data_guid
        )
        log_msg = ''
        if sub_data_guid:
            log_msg = f' - 从表sub_data_guid{sub_data_guid}'
        if not db_model:
            LOGGER.insert_db.warning(
                f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {stored_field} - 下载表guid:{data_guid}{log_msg} - 入库下载表失败 - 未能生成db_model!')
            raise Exception(
                f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {stored_field} - 下载表guid:{data_guid}{log_msg} - 入库下载表失败 - 未能生成db_model!')
            # return
        try:
            insert_sql, _args = db_model.get_insert_sql_to_sqlserver()
            self.PY_MSSQL.execute_sql(insert_sql, _args)
            LOGGER.insert_db.info(
                f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {stored_field} - 下载表guid:{data_guid}{log_msg} - 入库下载表成功!')
        except Exception as e:
            # 传统下载表没有加唯一索引
            if 'duplicate key' in str(e):
                LOGGER.update.info(
                    f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {stored_field} - 下载表guid:{data_guid}{log_msg} - 入库下载表失败 - 数据重复!')
                # self.MONGO.insert_one({"data_guid": data_guid, "sourceid": sourceid,
                #                        "modulecode": record['modulecode']})
            else:
                # LOGGER.insert_db.error(
                #     f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {stored_field} - 下载表guid:{data_guid}{log_msg} - 入库下载表失败 - {str(traceback.format_exc())}')
                raise Exception(
                    f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {stored_field} - 下载表guid:{data_guid}{log_msg} - 入库下载表失败 - {str(traceback.format_exc())}')
        else:
            pass
            # try:
            #     # 这里插入还要查询一波的原因是防止两个重复的披露数据在去重步骤都是需要入库，但是真正入库的时候应该只入一条才对
            #     # 插入前还要查一次，要采用存在就不处理，不存在即插入，而且要加锁，防止多个进程同时查询都不存在，
            #     self.MONGO.insert_one({
            #         "data_guid": data_guid,
            #         "sourceid": sourceid,
            #         "modulecode": record['modulecode']
            #     })
            # except Exception as e:
            #     # 增加处理逻辑：如果是需要去重的数据，在这个异常处捕捉到了唯一索引重复，那就抛异常
            #     # 如果是不需要去重的数据，在这个异常处捕捉到了唯一索引重复，那就不抛异常，
            #     LOGGER.insert_mongo.error(
            #         f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {stored_field} - 下载表guid:{data_guid} - 入库MongoDB失败 - {e}')
            #     raise Exception(
            #         f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {stored_field} - 下载表guid:{data_guid} - 入库MongoDB失败 - {e}')
            # else:
            #     LOGGER.insert_mongo.info(
            #         f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {stored_field} - 下载表guid:{data_guid} - 入库MongoDB成功!')

    def get_download_db_model(self, download_table_name, record, sourceid, grabdatacode, guid,
                              sub_data_guid):
        # # 消息体可能缺失
        self.check_record(record, download_table_name)
        db_model = None
        if isinstance(download_table_name, str):
            if download_table_name == 'CT_CFPNAV':
                db_model = CTCFPNAVModel(sitename=record['sitename'],
                                         sitesort1=record['sitesort'],
                                         sitesort2=record['sitesort2'],
                                         url=record['url'],
                                         ptsname=record['ptsname'],
                                         declaredate=record['declaredate'],
                                         updatedate=record['updatedate'],
                                         publishdate=record['publishdate'],
                                         currency=record['currency'],
                                         systemno=record['systemno'],
                                         state=0,
                                         memo=record['memo'],
                                         guid=guid,
                                         sourceid=sourceid,
                                         grabdatacode=grabdatacode
                                         )
                if record['cfpnav1']:
                    d = str(record['cfpnav1'])
                    if ',' in d or '，' in d:
                        LOGGER.insert_mongo.error(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：cfpnav1字段值转化为float失败：{d}')
                        raise Exception(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：cfpnav1字段值转化为float失败：{d}')
                    db_model.cfpnav1 = float(record['cfpnav1'])
                if record['cfpnav2']:
                    d = str(record['cfpnav2'])
                    if ',' in d or '，' in d:
                        LOGGER.insert_mongo.error(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：cfpnav2字段值转化为float失败：{d}')
                        raise Exception(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：- cfpnav2字段值转化为float失败：{d}')
                    db_model.cfpnav2 = float(record['cfpnav2'])
            elif download_table_name == 'CT_CFPNAV_CUR':
                db_model = CTCFPNAVCURModel(sitename=record['sitename'],
                                            sitesort1=record['sitesort'],
                                            url=record['url'],
                                            ptsname=record['ptsname'],
                                            declaredate=record['declaredate'],
                                            updatedate=record['updatedate'],
                                            enddate=record['enddate'],
                                            # begindate=record['begindate'],
                                            cfpnav_cur1=record['cfpnav_cur1'],
                                            systemno=record['systemno'],
                                            state=0,
                                            memo=record['memo'],
                                            guid=guid,
                                            sourceid=sourceid,
                                            grabdatacode=grabdatacode
                                            )
                if record['cfpnav_cur2']:
                    d = str(record['cfpnav_cur2'])
                    if ',' in d or '，' in d:
                        LOGGER.insert_mongo.error(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：cfpnav_cur2字段值转化为float失败：{d}')
                        raise Exception(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：cfpnav_cur2字段值转化为float失败：{d}')
                    db_model.cfpnav_cur2 = float(record['cfpnav_cur2'])
                if record['cfpnav_cur3']:
                    d = str(record['cfpnav_cur3'])
                    if ',' in d or '，' in d:
                        LOGGER.insert_mongo.error(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：cfpnav_cur3字段值转化为float失败：{d}')
                        raise Exception(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：cfpnav_cur3字段值转化为float失败：{d}')
                    db_model.cfpnav_cur3 = float(record['cfpnav_cur3'])
                if record['cfpnav_cur4']:
                    d = str(record['cfpnav_cur4'])
                    if ',' in d or '，' in d:
                        LOGGER.insert_mongo.error(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：cfpnav_cur4字段值转化为float失败：{d}')
                        raise Exception(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：cfpnav_cur4字段值转化为float失败：{d}')
                    db_model.cfpnav_cur4 = float(record['cfpnav_cur4'])
                if (not db_model.memo) and record.get('is_ckgm', ''):
                    db_model.memo = '参考公募'
            elif download_table_name == 'CT_PFPNAV':
                db_model = CTPFPNAVModel(sitename=record['sitename'],
                                         sitesort1=record['sitesort'],
                                         sitesort2=record['sitesort2'],
                                         url=record['url'],
                                         ptsname=record['ptsname'],
                                         declaredate=record['declaredate'],
                                         pfpnav1=record['pfpnav1'],
                                         pfproductcode=record['pfproductcode'],
                                         pfpnav9=record['pfpnav9'],
                                         pfpnav2=record['pfpnav2'],
                                         pfpnav8=record['pfpnav8'],
                                         systemno=record['systemno'],
                                         state=0,
                                         memo=record['memo'],
                                         guid=guid,
                                         sourceid=sourceid,
                                         grabdatacode=grabdatacode
                                         )
                if record['pfpnav3']:
                    d = str(record['pfpnav3'])
                    if ',' in d or '，' in d:
                        LOGGER.insert_mongo.error(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：pfpnav3字段值转化为float失败：{d}')
                        raise Exception(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：pfpnav3字段值转化为float失败：{d}')
                    db_model.pfpnav3 = float(record['pfpnav3'])
                if record['pfpnav4']:
                    d = str(record['pfpnav4'])
                    if ',' in d or '，' in d:
                        LOGGER.insert_mongo.error(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：pfpnav4字段值转化为float失败：{d}')
                        raise Exception(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：pfpnav4字段值转化为float失败：{d}')
                    db_model.pfpnav4 = float(record['pfpnav4'])
                if record['pfpnav5']:
                    d = str(record['pfpnav5'])
                    if ',' in d or '，' in d:
                        LOGGER.insert_mongo.error(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：pfpnav5字段值转化为float失败：{d}')
                        raise Exception(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：pfpnav5字段值转化为float失败：{d}')
                    db_model.pfpnav5 = float(record['pfpnav5'])
                if record['pfpnav6']:
                    d = str(record['pfpnav6'])
                    if ',' in d or '，' in d:
                        raise Exception(f'pfpnav6字段值转化为float失败：{d}')
                    db_model.pfpnav6 = float(record['pfpnav6'])
                if record['pfpnav7']:
                    d = str(record['pfpnav7'])
                    if ',' in d or '，' in d:
                        LOGGER.insert_mongo.error(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：pfpnav7字段值转化为float失败：{d}')
                        raise Exception(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：pfpnav7字段值转化为float失败：{d}')
                    db_model.pfpnav7 = float(record['pfpnav7'])
            elif download_table_name == 'CT_PFPNav_CUR':
                db_model = CTPFPNavCURModel(sitename=record['sitename'],
                                            sitesort1=record['sitesort'],
                                            sitesort2=record['sitesort2'],
                                            url=record['url'],
                                            pfproductname=record['pfproductname'],
                                            declaredate=record['declaredate'],
                                            updatedate=record['updatedate'],
                                            enddate=record['enddate'],
                                            systemno=record['systemno'],
                                            state=0,
                                            pfpnav_cur5=record['pfpnav_cur5'],
                                            pfpnav_cur1=record['pfpnav_cur1'],
                                            memo=record['memo'],
                                            guid=guid,
                                            sourceid=sourceid,
                                            grabdatacode=grabdatacode
                                            )
                if record['pfpnav_cur2']:
                    d = str(record['pfpnav_cur2'])
                    if ',' in d or '，' in d:
                        LOGGER.insert_mongo.error(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：pfpnav_cur2字段值转化为float失败：{d}')
                        raise Exception(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：pfpnav_cur2字段值转化为float失败：{d}')
                    db_model.pfpnav_cur2 = float(record['pfpnav_cur2'])
                if record['pfpnav_cur3']:
                    d = str(record['pfpnav_cur3'])
                    if ',' in d or '，' in d:
                        LOGGER.insert_mongo.error(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：pfpnav_cur3字段值转化为float失败：{d}')
                        raise Exception(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：pfpnav_cur3字段值转化为float失败：{d}')
                    db_model.pfpnav_cur3 = float(record['pfpnav_cur3'])
                if record['pfpnav_cur4']:
                    d = str(record['pfpnav_cur4'])
                    if ',' in d or '，' in d:
                        LOGGER.insert_mongo.error(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：pfpnav_cur4字段值转化为float失败：{d}')
                        raise Exception(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：pfpnav_cur4字段值转化为float失败：{d}')
                    db_model.pfpnav_cur4 = float(record['pfpnav_cur4'])
            elif download_table_name == 'CT_Iperunit':
                db_model = CTIperunitModel(sitename=record['sitename'],
                                           sitesort1=record['sitesort'],
                                           sitesort2=record['sitesort2'],
                                           url=record['url'],
                                           ptsname=record['ptsname'],
                                           declaredate=record['declaredate'],
                                           iperunit1=record['iperunit1'],
                                           systemno=record['systemno'],
                                           state=0,
                                           guid=guid,
                                           sourceid=sourceid,
                                           grabdatacode=grabdatacode
                                           )
                if record['iperunit2']:
                    d = str(record['iperunit2'])
                    if ',' in d or '，' in d:
                        LOGGER.insert_mongo.error(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：iperunit2字段值转化为float失败：{d}')
                        raise Exception(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：iperunit2字段值转化为float失败：{d}')
                    db_model.iperunit2 = float(record['iperunit2'])
                if record['iperunit3']:
                    d = str(record['iperunit3'])
                    if ',' in d or '，' in d:
                        LOGGER.insert_mongo.error(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：iperunit3字段值转化为float失败：{d}')
                        raise Exception(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：iperunit3字段值转化为float失败：{d}')
                    db_model.iperunit3 = float(record['iperunit3'])
                if record['iperunit4']:
                    d = str(record['iperunit4'])
                    if ',' in d or '，' in d:
                        LOGGER.insert_mongo.error(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：iperunit4字段值转化为float失败：{d}')
                        raise Exception(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：iperunit4字段值转化为float失败：{d}')
                    db_model.iperunit4 = float(record['iperunit4'])
            elif download_table_name == 'CT_IPerPrice':
                db_model = CTIPerPriceModel(sitename=record['sitename'],
                                            sitesort1=record['sitesort'],
                                            sitesort2=record['sitesort2'],
                                            url=record['url'],
                                            insperaname=record['insperaname'],
                                            declaredate=record['declaredate'],
                                            iperprice2=record['iperprice2'],
                                            systemno=record['systemno'],
                                            state=0,
                                            memo=record['memo'],
                                            guid=guid,
                                            sourceid=sourceid,
                                            grabdatacode=grabdatacode
                                            )
                if record['iperprice3']:
                    d = str(record['iperprice3'])
                    if ',' in d or '，' in d:
                        LOGGER.insert_mongo.error(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：iperprice3字段值转化为float失败：{d}')
                        raise Exception(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：iperprice3字段值转化为float失败：{d}')
                    db_model.iperprice3 = float(record['iperprice3'])
                if record['iperprice4']:
                    d = str(record['iperprice4'])
                    if ',' in d or '，' in d:
                        LOGGER.insert_mongo.error(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：iperprice4字段值转化为float失败：{d}')
                        raise Exception(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：iperprice4字段值转化为float失败：{d}')
                    db_model.iperprice4 = float(record['iperprice4'])
            elif download_table_name == 'CT_InsRate':
                db_model = CTInsRateModel(sitename=record['sitename'],
                                          sitesort1=record['sitesort'],
                                          sitesort2=record['sitesort2'],
                                          url=record['url'],
                                          insname=record['insname'],
                                          declaredate=record['declaredate'],
                                          updatedate=record['updatedate'],
                                          insrate1=record['insrate1'],
                                          insrate8=record['insrate8'],
                                          systemno=record['systemno'],
                                          state=0,
                                          guid=guid,
                                          sourceid=sourceid,
                                          grabdatacode=grabdatacode
                                          )
                if record['insrate2']:
                    d = str(record['insrate2'])
                    if ',' in d or '，' in d:
                        LOGGER.insert_mongo.error(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：insrate2字段值转化为float失败：{d}')
                        raise Exception(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：insrate2字段值转化为float失败：{d}')
                    db_model.insrate2 = float(record['insrate2'])
                if record['insrate3']:
                    d = str(record['insrate3'])
                    if ',' in d or '，' in d:
                        LOGGER.insert_mongo.error(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：insrate3字段值转化为float失败：{d}')
                        raise Exception(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：insrate3字段值转化为float失败：{d}')
                    db_model.insrate3 = float(record['insrate3'])
                if record['insrate4']:
                    d = str(record['insrate4'])
                    if ',' in d or '，' in d:
                        LOGGER.insert_mongo.error(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：insrate4字段值转化为float失败：{d}')
                        raise Exception(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：insrate4字段值转化为float失败：{d}')
                    db_model.insrate4 = float(record['insrate4'])
                if record['insrate5']:
                    d = str(record['insrate5'])
                    if ',' in d or '，' in d:
                        LOGGER.insert_mongo.error(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：insrate5字段值转化为float失败：{d}')
                        raise Exception(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：insrate5字段值转化为float失败：{d}')
                    db_model.insrate5 = float(record['insrate5'])
                if record['insrate6']:
                    d = str(record['insrate6'])
                    if ',' in d or '，' in d:
                        LOGGER.insert_mongo.error(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：insrate6字段值转化为float失败：{d}')
                        raise Exception(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：insrate6字段值转化为float失败：{d}')
                    db_model.insrate6 = float(record['insrate6'])
                if record['insrate7']:
                    d = str(record['insrate7'])
                    if ',' in d or '，' in d:
                        LOGGER.insert_mongo.error(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：insrate7字段值转化为float失败：{d}')
                        raise Exception(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：insrate7字段值转化为float失败：{d}')
                    db_model.insrate7 = float(record['insrate7'])

                # # 根据这个字段来InsRate1 取月初日期
                # begindate = record['begindate'],
                # # 月末一天
                # enddate = record['enddate'],
            elif download_table_name == 'CT_PFNAV':
                db_model = CTPFNAVModel(sitename=record['sitename'],
                                        sitesort1=record['sitesort'],
                                        sitesort2=record['sitesort2'],
                                        url=record['url'],
                                        pfaname=record['pfaname'],
                                        fwdate=record['fwdate'],
                                        updatedate=record['updatedate'],
                                        pfcode=record['pfcode'],
                                        publishdate=record['publishdate'],
                                        systemno=record['systemno'],
                                        state=0,
                                        guid=guid,
                                        sourceid=sourceid,
                                        grabdatacode=grabdatacode
                                        )
                if record['pfnav1']:
                    d = str(record['pfnav1'])
                    if ',' in d or '，' in d:
                        LOGGER.insert_mongo.error(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：pfnav1字段值转化为float失败：{d}')
                        raise Exception(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：pfnav1字段值转化为float失败：{d}')
                    db_model.pfnav1 = float(record['pfnav1'])
                if record['pfnav2']:
                    d = str(record['pfnav2'])
                    if ',' in d or '，' in d:
                        LOGGER.insert_mongo.error(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：pfnav2字段值转化为float失败：{d}')
                        raise Exception(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：pfnav2字段值转化为float失败：{d}')
                    db_model.pfnav2 = float(record['pfnav2'])
            elif download_table_name == 'CT_PFNAV_CUR':
                db_model = CTPFNAVCURModel(sitename=record['sitename'],
                                           sitesort1=record['sitesort'],
                                           sitesort2=record['sitesort2'],
                                           url=record['url'],
                                           pfaname=record['pfaname'],
                                           fwdate=record['fwdate'],
                                           updatedate=record['updatedate'],
                                           pfcode=record['pfcode'],
                                           enddate=record['enddate'],
                                           systemno=record['systemno'],
                                           state=0,
                                           guid=guid,
                                           sourceid=sourceid,
                                           grabdatacode=grabdatacode
                                           )
                if record['pfnav_cur2']:
                    d = str(record['pfnav_cur2'])
                    if ',' in d or '，' in d:
                        LOGGER.insert_mongo.error(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：pfnav_cur2字段值转化为float失败：{d}')
                        raise Exception(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：pfnav_cur2字段值转化为float失败：{d}')
                    db_model.pfnav_cur2 = float(record['pfnav_cur2'])
                if record['pfnav_cur3']:
                    d = str(record['pfnav_cur3'])
                    if ',' in d or '，' in d:
                        LOGGER.insert_mongo.error(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：pfnav_cur3字段值转化为float失败：{d}')
                        raise Exception(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：pfnav_cur3字段值转化为float失败：{d}')
                    db_model.pfnav_cur3 = float(record['pfnav_cur3'])
            elif download_table_name == 'CT_NAV_All':
                db_model = CTNAVAllModel(sitename=record['sitename'],
                                         sitesort1=record['sitesort'],
                                         sitesort2=record['sitesort2'],
                                         url=record['url'],
                                         symbol=record['symbol'],
                                         webname=record['webname'],
                                         publishdate=record['publishdate'],
                                         datasource=record['datasource'],
                                         isautomatic=record['isautomatic'],
                                         systemno=record['systemno'],
                                         state=0,
                                         guid=guid,
                                         sourceid=sourceid,
                                         grabdatacode=grabdatacode
                                         )
                if record['nav1']:
                    d = str(record['nav1'])
                    if ',' in d or '，' in d:
                        LOGGER.insert_mongo.error(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：nav1字段值转化为float失败：{d}')
                        raise Exception(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：nav1字段值转化为float失败：{d}')
                    db_model.nav1 = float(record['nav1'])
                if record['nav2']:
                    d = str(record['nav2'])
                    if ',' in d or '，' in d:
                        LOGGER.insert_mongo.error(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：nav2字段值转化为float失败：{d}')
                        raise Exception(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：nav2字段值转化为float失败：{d}')
                    db_model.nav2 = float(record['nav2'])
            elif download_table_name == 'CT_NAV_CUR_All':
                db_model = CTNAVCURAllModel(sitename=record['sitename'],
                                            sitesort1=record['sitesort'],
                                            sitesort2=record['sitesort2'],
                                            url=record['url'],
                                            symbol=record['symbol'],
                                            webname=record['webname'],
                                            publishdate=record['publishdate'],
                                            datasource=record['datasource'],
                                            isautomatic=record['isautomatic'],
                                            nav_cur1=record['nav_cur1'],
                                            systemno=record['systemno'],
                                            state=0,
                                            guid=guid,
                                            sourceid=sourceid,
                                            grabdatacode=grabdatacode
                                            )
                if record['nav_cur4']:
                    d = str(record['nav_cur4'])
                    if ',' in d or '，' in d:
                        LOGGER.insert_mongo.error(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：nav_cur4字段值转化为float失败：{d}')
                        raise Exception(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：nav_cur4字段值转化为float失败：{d}')
                    db_model.nav_cur4 = float(record['nav_cur4'])
                if record['nav_cur5']:
                    d = str(record['nav_cur5'])
                    if ',' in d or '，' in d:
                        LOGGER.insert_mongo.error(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：nav_cur5字段值转化为float失败：{d}')
                        raise Exception(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：nav_cur5字段值转化为float失败：{d}')
                    db_model.nav_cur5 = float(record['nav_cur5'])
            elif download_table_name == 'CT_TRUSTNAV':
                db_model = CTTRUSTNAVModel(sitename=record['sitename'],
                                           sitesort1=record['sitesort'],
                                           sitesort2=record['sitesort2'],
                                           sourceurl=record['sourceurl'],
                                           trustaname=record['trustaname'],
                                           updatedate=record['updatedate'],
                                           declaredate=record['declaredate'],
                                           publishdate=record['publishdate'],
                                           trustnav1=record['trustnav1'],
                                           memo=record['memo'],
                                           systemno=record['systemno'],
                                           state=0,
                                           guid=guid,
                                           sourceid=sourceid,
                                           grabdatacode=grabdatacode
                                           )
                if record['trustnav4']:
                    d = str(record['trustnav4'])
                    if ',' in d or '，' in d:
                        LOGGER.insert_mongo.error(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：trustnav4字段值转化为float失败：{d}')
                        raise Exception(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：trustnav4字段值转化为float失败：{d}')
                    db_model.trustnav4 = float(record['trustnav4'])
                if record['trustnav5']:
                    d = str(record['trustnav5'])
                    if ',' in d or '，' in d:
                        LOGGER.insert_mongo.error(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：trustnav5字段值转化为float失败：{d}')
                        raise Exception(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：trustnav5字段值转化为float失败：{d}')
                    db_model.trustnav5 = float(record['trustnav5'])
                if record['trustnav6']:
                    d = str(record['trustnav6'])
                    if ',' in d or '，' in d:
                        LOGGER.insert_mongo.error(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：trustnav6字段值转化为float失败：{d}')
                        raise Exception(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：trustnav6字段值转化为float失败：{d}')
                    db_model.trustnav6 = float(record['trustnav6'])
            elif download_table_name == 'CT_TRUSTNAV_cur':
                db_model = CTTRUSTNAVCurModel(sitename=record['sitename'],
                                              sitesort1=record['sitesort'],
                                              sitesort2=record['sitesort2'],
                                              url=record['url'],
                                              trustaname=record['trustaname'],
                                              updatedate=record['updatedate'],
                                              declaredate=record['declaredate'],
                                              enddate=record['enddate'],
                                              systemno=record['systemno'],
                                              state=0,
                                              guid=guid,
                                              sourceid=sourceid,
                                              grabdatacode=grabdatacode
                                              )
                if record['trustnav_cur2']:
                    d = str(record['trustnav_cur2'])
                    if ',' in d or '，' in d:
                        LOGGER.insert_mongo.error(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：trustnav_cur2字段值转化为float失败：{d}')
                        raise Exception(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：trustnav_cur2字段值转化为float失败：{d}')
                    db_model.trustnav_cur2 = float(record['trustnav_cur2'])
                if record['trustnav_cur3']:
                    d = str(record['trustnav_cur3'])
                    if ',' in d or '，' in d:
                        LOGGER.insert_mongo.error(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：trustnav_cur3字段值转化为float失败：{d}')
                        raise Exception(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：trustnav_cur3字段值转化为float失败：{d}')
                    db_model.trustnav_cur3 = float(record['trustnav_cur3'])
                if record['trustnav_cur4']:
                    d = str(record['trustnav_cur4'])
                    if ',' in d or '，' in d:
                        LOGGER.insert_mongo.error(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：trustnav_cur4字段值转化为float失败：{d}')
                        raise Exception(
                            f'配置表sourceid:{sourceid} - {DATA_STORE_FLAG} - {record} - 下载表guid:{guid} - 下载表入库失败：trustnav_cur4字段值转化为float失败：{d}')
                    db_model.trustnav_cur4 = float(record['trustnav_cur4'])
            elif download_table_name == 'CT_CgRecJJYXH':
                db_model = CTCgRecJJYXHModel(sitename=record['sitename'],
                                             sitesort1=record['sitesort'],
                                             sitesort2=record['sitesort2'],
                                             url=record['url'],
                                             publishdate=record['publishdate'],
                                             updatedate=record['updatedate'],
                                             cgrec1=record['cgrec1'],
                                             filepath=record['filepath'],
                                             systemno=record['systemno'],
                                             state=0,
                                             guid=guid,
                                             sourceid=sourceid,
                                             grabdatacode=grabdatacode
                                             )
        elif isinstance(download_table_name, list):
            if download_table_name[0] == 'CT_PFPAnnounMT':
                if not sub_data_guid:
                    # 主表数据
                    db_model = CTPFPAnnounMTModel(sitename=record['sitename'],
                                                  sitesort1=record['sitesort'],
                                                  sitesort2=record['sitesort2'],
                                                  url=record['url'],
                                                  title=record['title'],
                                                  declaredate=record['declaredate'],
                                                  uniquecode=guid,
                                                  content=record['content'],
                                                  systemno=record['systemno'],
                                                  state=0,
                                                  guid=guid,
                                                  sourceid=sourceid,
                                                  grabdatacode=grabdatacode
                                                  )
                else:
                    db_model = CTPFPAnnounMTFILEModel(downfileurl=record['downfileurl'],
                                                      downfilename=record['downfilename'],
                                                      downfilelist=record['downfilelist'],
                                                      uniquecode=guid,
                                                      parentguid=guid,
                                                      spiderguid=sub_data_guid,
                                                      sourceid=sourceid,
                                                      grabdatacode=grabdatacode,
                                                      state=0
                                                      )
            elif download_table_name[0] == 'CT_CfpAnnounceMent':
                if not sub_data_guid:
                    # 主表数据
                    db_model = CTCfpAnnounceMentModel(sitename=record['sitename'],
                                                      sitesort1=record['sitesort'],
                                                      sitesort2=record['sitesort2'],
                                                      url=record['url'],
                                                      title=record['title'],
                                                      declaredate=record['declaredate'],
                                                      uniquecode=guid,
                                                      content=record['content'],
                                                      systemno=record['systemno'],
                                                      memo1=record['memo1'],
                                                      state=0,
                                                      guid=guid,
                                                      sourceid=sourceid,
                                                      grabdatacode=grabdatacode,
                                                      )
                else:
                    db_model = CTCfpAnnounceMentFileModel(downfileurl=record['downfileurl'],
                                                          downfilename=record['downfilename'],
                                                          downfilelist=record['downfilelist'],
                                                          uniquecode=guid,
                                                          parentguid=guid,
                                                          spiderguid=sub_data_guid,
                                                          sourceid=sourceid,
                                                          grabdatacode=grabdatacode,
                                                          state=0
                                                          )

        return db_model

    def check_record(self, record, download_table_name):
        if record.get("guid", None) is None:
            record['guid'] = ''

        if record.get("sourceid", None) is None:
            record['sourceid'] = ''

        if record.get("grabdatacode", None) is None:
            record['grabdatacode'] = ''

        if record.get("sitename", None) is None:
            record['sitename'] = ''

        # sitesort就是下载表的sitesort1
        if record.get("sitesort1", None) is None:
            if record.get("sitesort", None) is None:
                record['sitesort'] = ''
        else:
            record['sitesort'] = record['sitesort1']

        if record.get("modulecode", None) is None:
            record['modulecode'] = ''

        if record.get("is_ckgm", None) is None:
            record['is_ckgm'] = ''

        if record.get("systemno", None) is None:
            record['systemno'] = ''

        if record.get("state", None) is None:
            record['state'] = 0
        if isinstance(download_table_name, list):
            if download_table_name[0] == 'CT_PFPAnnounMT':
                self.check_ct_pfpannounmt_tab_column(record)
            elif download_table_name[0] == 'CT_CfpAnnounceMent':
                self.check_ct_cfpannounmt_tab_column(record)
        else:
            if download_table_name == 'CT_CFPNAV':
                self.check_ct_cfpnav_tab_column(record)
            elif download_table_name == 'CT_CFPNAV_CUR':
                self.check_ct_cfpnav_cur_tab_column(record)
            elif download_table_name == 'CT_PFPNAV':
                self.check_ct_pfpnav_tab_column(record)
            elif download_table_name == 'CT_PFPNav_CUR':
                self.check_ct_pfpnav_cur_tab_column(record)
            elif download_table_name == 'CT_Iperunit':
                # 保险价格
                self.check_ct_iperunit_tab_column(record)
            elif download_table_name == 'CT_IPerPrice':
                self.check_ct_iperprice_tab_column(record)
            elif download_table_name == 'CT_InsRate':
                self.check_ct_insiate_tab_column(record)
            elif download_table_name == 'CT_PFNAV':
                self.check_ct_pfnav_tab_column(record)
            elif download_table_name == 'CT_PFNAV_CUR':
                self.check_ct_pfnav_cur_tab_column(record)
            elif download_table_name == 'CT_NAV_All':
                self.check_ct_nav_all_tab_column(record)
            elif download_table_name == 'CT_NAV_CUR_All':
                self.check_ct_nav_cur_all_tab_column(record)
            elif download_table_name == 'CT_TRUSTNAV':
                self.check_ct_trustnav_tab_column(record)
            elif download_table_name == 'CT_TRUSTNAV_cur':
                self.check_ct_trustnav_cur_tab_column(record)
            elif download_table_name == 'CT_CgRecJJYXH':
                self.check_ct_cgrecjjyxh_tab_column(record)

    @staticmethod
    def check_ct_cfpnav_tab_column(record):
        if record.get("url", None) is None:
            record['url'] = ''

        if record.get("sitesort2", None) is None:
            record['sitesort2'] = ''

        if record.get("ptsname", None) is None:
            record['ptsname'] = ''

        if record.get("declaredate", None) is None:
            record['declaredate'] = ''

        if record.get("updatedate", None) is None:
            record['updatedate'] = ''

        if record.get("publishdate", None) is None:
            record['publishdate'] = ''

        if record.get("cfpnav1", None) is None:
            record['cfpnav1'] = ''

        if record.get("cfpnav2", None) is None:
            record['cfpnav2'] = ''

        if record.get("currency", None) is None:
            record['currency'] = ''

        if record.get("memo", None) is None:
            record['memo'] = ''

    @staticmethod
    def check_ct_cfpnav_cur_tab_column(record):
        if record.get("url", None) is None:
            record['url'] = ''

        if record.get("sitesort2", None) is None:
            record['sitesort2'] = ''

        if record.get("ptsname", None) is None:
            record['ptsname'] = ''

        if record.get("declaredate", None) is None:
            record['declaredate'] = ''

        if record.get("updatedate", None) is None:
            record['updatedate'] = ''

        if record.get("enddate", None) is None:
            record['enddate'] = ''

        if record.get("cfpnav_cur1", None) is None:
            record['cfpnav_cur1'] = ''

        if record.get("cfpnav_cur2", None) is None:
            record['cfpnav_cur2'] = ''

        if record.get("cfpnav_cur3", None) is None:
            record['cfpnav_cur3'] = ''

        if record.get("cfpnav_cur4", None) is None:
            record['cfpnav_cur4'] = ''

        if record.get("memo", None) is None:
            record['memo'] = ''

    @staticmethod
    def check_ct_pfpnav_tab_column(record):
        if record.get("url", None) is None:
            record['url'] = ''

        if record.get("sitesort2", None) is None:
            record['sitesort2'] = ''

        if record.get("ptsname", None) is None:
            record['ptsname'] = ''

        if record.get("pfpnav9", None) is None:
            record['pfpnav9'] = ''

        if record.get("pfproductcode", None) is None:
            record['pfproductcode'] = ''

        if record.get("declaredate", None) is None:
            record['declaredate'] = ''

        if record.get("pfpnav1", None) is None:
            record['pfpnav1'] = ''

        if record.get("pfpnav2", None) is None:
            record['pfpnav2'] = ''

        if record.get("pfpnav3", None) is None:
            record['pfpnav3'] = ''

        if record.get("pfpnav4", None) is None:
            record['pfpnav4'] = ''

        if record.get("pfpnav5", None) is None:
            record['pfpnav5'] = ''

        if record.get("pfpnav6", None) is None:
            record['pfpnav6'] = ''

        if record.get("pfpnav7", None) is None:
            record['pfpnav7'] = ''

        if record.get("pfpnav8", None) is None:
            record['pfpnav8'] = ''

        if record.get("memo", None) is None:
            record['memo'] = ''

    @staticmethod
    def check_ct_pfpnav_cur_tab_column(record):
        if record.get("url", None) is None:
            record['url'] = ''

        if record.get("sitesort2", None) is None:
            record['sitesort2'] = ''

        if record.get("pfproductname", None) is None:
            record['pfproductname'] = ''

        if record.get("declaredate", None) is None:
            record['declaredate'] = ''

        if record.get("updatedate", None) is None:
            record['updatedate'] = ''

        if record.get("enddate", None) is None:
            record['enddate'] = ''

        if record.get("pfpnav_cur1", None) is None:
            record['pfpnav_cur1'] = ''

        if record.get("pfpnav_cur2", None) is None:
            record['pfpnav_cur2'] = ''

        if record.get("pfpnav_cur3", None) is None:
            record['pfpnav_cur3'] = ''

        if record.get("pfpnav_cur4", None) is None:
            record['pfpnav_cur4'] = ''

        if record.get("pfpnav_cur5", None) is None:
            record['pfpnav_cur5'] = ''

        if record.get("memo", None) is None:
            record['memo'] = ''

    @staticmethod
    def check_ct_iperunit_tab_column(record):
        if record.get("url", None) is None:
            record['url'] = ''

        if record.get("sitesort2", None) is None:
            record['sitesort2'] = ''

        if record.get("ptsname", None) is None:
            record['ptsname'] = ''

        if record.get("declaredate", None) is None:
            record['declaredate'] = ''

        if record.get("iperunit1", None) is None:
            record['iperunit1'] = ''

        if record.get("iperunit2", None) is None:
            record['iperunit2'] = ''

        if record.get("iperunit3", None) is None:
            record['iperunit3'] = ''

        if record.get("iperunit4", None) is None:
            record['iperunit4'] = ''

        if record.get("memo", None) is None:
            record['memo'] = ''

    @staticmethod
    def check_ct_iperprice_tab_column(record):
        if record.get("url", None) is None:
            record['url'] = ''

        if record.get("sitesort2", None) is None:
            record['sitesort2'] = ''

        if record.get("insperaname", None) is None:
            record['insperaname'] = ''

        if record.get("declaredate", None) is None:
            record['declaredate'] = ''

        if record.get("iperprice2", None) is None:
            record['iperprice2'] = ''

        if record.get("iperprice3", None) is None:
            record['iperprice3'] = ''

        if record.get("iperprice4", None) is None:
            record['iperprice4'] = ''

        if record.get("memo", None) is None:
            record['memo'] = ''

    @staticmethod
    def check_ct_insiate_tab_column(record):
        if record.get("url", None) is None:
            record['url'] = ''

        if record.get("declaredate", None) is None:
            record['declaredate'] = ''

        if record.get("updatedate", None) is None:
            record['updatedate'] = ''

        if record.get("insname", None) is None:
            record['insname'] = ''

        if record.get("insrate1", None) is None:
            record['insrate1'] = ''

        if record.get("begindate", None) is None:
            record['begindate'] = ''

        if record.get("enddate", None) is None:
            record['enddate'] = ''

        if record.get("insrate2", None) is None:
            record['insrate2'] = ''

        if record.get("insrate3", None) is None:
            record['insrate3'] = ''

        if record.get("insrate4", None) is None:
            record['insrate4'] = ''

        if record.get("insrate5", None) is None:
            record['insrate5'] = ''

        if record.get("insrate6", None) is None:
            record['insrate6'] = ''

        if record.get("insrate7", None) is None:
            record['insrate7'] = ''

        if record.get("insrate8", None) is None:
            record['insrate8'] = ''

    @staticmethod
    def check_ct_pfnav_tab_column(record):
        if record.get("url", None) is None:
            record['url'] = ''

        if record.get("sitesort2", None) is None:
            record['sitesort2'] = ''

        if record.get("fwdate", None) is None:
            record['fwdate'] = ''

        if record.get("updatedate", None) is None:
            record['updatedate'] = ''

        if record.get("pfaname", None) is None:
            record['pfaname'] = ''

        if record.get("pfcode", None) is None:
            record['pfcode'] = ''

        if record.get("publishdate", None) is None:
            record['publishdate'] = ''

        if record.get("pfnav1", None) is None:
            record['pfnav1'] = ''

        if record.get("pfnav2", None) is None:
            record['pfnav2'] = ''

    @staticmethod
    def check_ct_pfnav_cur_tab_column(record):
        if record.get("url", None) is None:
            record['url'] = ''

        if record.get("sitesort2", None) is None:
            record['sitesort2'] = ''

        if record.get("fwdate", None) is None:
            record['fwdate'] = ''

        if record.get("updatedate", None) is None:
            record['updatedate'] = ''

        if record.get("pfaname", None) is None:
            record['pfaname'] = ''

        if record.get("enddate", None) is None:
            record['enddate'] = ''

        if record.get("pfnav_cur2", None) is None:
            record['pfnav_cur2'] = ''

        if record.get("pfnav_cur3", None) is None:
            record['pfnav_cur3'] = ''

    @staticmethod
    def check_ct_nav_all_tab_column(record):
        if record.get("url", None) is None:
            record['url'] = ''

        if record.get("sitesort2", None) is None:
            record['sitesort2'] = ''

        if record.get("symbol", None) is None:
            record['symbol'] = ''

        if record.get("webname", None) is None:
            record['webname'] = ''

        if record.get("publishdate", None) is None:
            record['publishdate'] = ''

        if record.get("nav1", None) is None:
            record['nav1'] = ''

        if record.get("nav2", None) is None:
            record['nav2'] = ''

        if record.get("datasource", None) is None:
            record['datasource'] = ''

        if record.get("isautomatic", None) is None:
            record['isautomatic'] = ''

    @staticmethod
    def check_ct_nav_cur_all_tab_column(record):
        if record.get("url", None) is None:
            record['url'] = ''

        if record.get("sitesort2", None) is None:
            record['sitesort2'] = ''

        if record.get("symbol", None) is None:
            record['symbol'] = ''

        if record.get("webname", None) is None:
            record['webname'] = ''

        if record.get("publishdate", None) is None:
            record['publishdate'] = ''

        if record.get("nav_cur1", None) is None:
            record['nav_cur1'] = ''

        if record.get("nav_cur4", None) is None:
            record['nav_cur4'] = ''

        if record.get("nav_cur5", None) is None:
            record['nav_cur5'] = ''

        if record.get("datasource", None) is None:
            record['datasource'] = ''

        if record.get("isautomatic", None) is None:
            record['isautomatic'] = ''

        if record.get("memo", None) is None:
            record['memo'] = ''

    @staticmethod
    def check_ct_trustnav_tab_column(record):
        if record.get("sourceurl", None) is None:
            record['sourceurl'] = ''

        if record.get("sitesort2", None) is None:
            record['sitesort2'] = ''

        if record.get("trustaname", None) is None:
            record['trustaname'] = ''

        if record.get("declaredate", None) is None:
            record['declaredate'] = ''

        if record.get("updatedate", None) is None:
            record['updatedate'] = ''

        if record.get("publishdate", None) is None:
            record['publishdate'] = ''

        if record.get("trustnav1", None) is None:
            record['trustnav1'] = ''

        if record.get("trustnav4", None) is None:
            record['trustnav4'] = ''

        if record.get("trustnav5", None) is None:
            record['trustnav5'] = ''

        if record.get("trustnav6", None) is None:
            record['trustnav6'] = ''

        if record.get("memo", None) is None:
            record['memo'] = ''

    @staticmethod
    def check_ct_trustnav_cur_tab_column(record):
        if record.get("url", None) is None:
            record['url'] = ''

        if record.get("sitesort2", None) is None:
            record['sitesort2'] = ''

        if record.get("declaredate", None) is None:
            record['declaredate'] = ''

        if record.get("updatedate", None) is None:
            record['updatedate'] = ''

        if record.get("trustaname", None) is None:
            record['trustaname'] = ''

        if record.get("enddate", None) is None:
            record['enddate'] = ''

        if record.get("trustnav_cur2", None) is None:
            record['trustnav_cur2'] = ''

        if record.get("trustnav_cur3", None) is None:
            record['trustnav_cur3'] = ''

        if record.get("trustnav_cur4", None) is None:
            record['trustnav_cur4'] = ''

        if record.get("memo", None) is None:
            record['memo'] = ''

    @staticmethod
    def check_ct_cgrecjjyxh_tab_column(record):
        if record.get("url", None) is None:
            record['url'] = ''

        if record.get("sitesort2", None) is None:
            record['sitesort2'] = ''

        if record.get("publishdate", None) is None:
            record['publishdate'] = ''

        if record.get("updatedate", None) is None:
            record['updatedate'] = ''

        if record.get("cgrec1", None) is None:
            record['cgrec1'] = ''

        if record.get("filepath", None) is None:
            record['filepath'] = ''

    @staticmethod
    def check_ct_pfpannounmt_tab_column(record):
        if record.get("url", None) is None:
            record['url'] = ''

        if record.get("sitesort2", None) is None:
            record['sitesort2'] = ''

        if record.get("title", None) is None:
            record['title'] = ''

        if record.get("declaredate", None) is None:
            record['declaredate'] = ''

        if record.get("updatedate", None) is None:
            record['updatedate'] = ''

        if record.get("uniquecode", None) is None:
            record['uniquecode'] = ''

        if record.get("content", None) is None:
            record['content'] = ''

        if record.get("downfileurl", None) is None:
            record['downfileurl'] = ''

        if record.get("downfilelist", None) is None:
            record['downfilelist'] = ''

        if record.get("downfilename", None) is None:
            record['downfilename'] = ''

        if record.get("memo", None) is None:
            record['memo'] = ''

    @staticmethod
    def check_ct_cfpannounmt_tab_column(record):
        if record.get("url", None) is None:
            record['url'] = ''

        if record.get("sitesort2", None) is None:
            record['sitesort2'] = ''

        if record.get("title", None) is None:
            record['title'] = ''

        if record.get("declaredate", None) is None:
            record['declaredate'] = ''

        if record.get("updatedate", None) is None:
            record['updatedate'] = ''

        if record.get("uniquecode", None) is None:
            record['uniquecode'] = ''

        if record.get("content", None) is None:
            record['content'] = ''

        if record.get("downfileurl", None) is None:
            record['downfileurl'] = ''

        if record.get("downfilelist", None) is None:
            record['downfilelist'] = ''

        if record.get("downfilename", None) is None:
            record['downfilename'] = ''

        if record.get("memo1", None) is None:
            record['memo1'] = ''


def consume_callback(ch, method, properties, body):
    # print('ch:', ch)
    # print('method:', method)
    # print('properties:', properties)
    # print('body', body)
    body_temp = ''
    try:
        body = body.decode('utf-8')
        body_dict = json.loads(body)
        body_temp = body_dict
        data_store = DataStored(OhMyExtractor, MONGO, OPSQL)
        data_store.run(body_dict)
    except Exception as e:
        LOGGER.other_op.warning(f'消息消费失败：{str(traceback.format_exc())},消息体：{body_temp}')
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    else:
        ch.basic_ack(delivery_tag=method.delivery_tag)
        LOGGER.receive_message.info(f"00 - [tag]{method.delivery_tag} -消息已确认消费")


def data_stored():
    global MONGO, OPSQL
    # data_store = DataStored(OhMyExtractor, MONGO, OPSQL)
    listener.start()  # 启动发送线程
    macip = OhMyExtractor.get_ip()
    assert macip
    LOGGER.other_op.debug(f'00 - 当前macip:{macip}')
    LOGGER.other_op.info("00 - 下载表数据入库程序启动!")

    # pool = ThreadPoolExecutor(max_workers=10)
    # record_queue = queue.Queue(maxsize=10)

    if db_env == '185':
        queue_name = TEST_LC_JZ_DATA_TO_BE_STORED_QUEUE_NAME
    elif db_env == 'formal':
        queue_name = LC_JZ_DATA_TO_BE_STORED_QUEUE_NAME
    else:
        raise Exception(f'未知的运行环境：{db_env}')

    try:
        global MQ_Manager
        if not MQ_Manager:
            MQ_Manager = RabbitMqManager(mq_env)
        MQ_Manager.queue_declare(queue_name=queue_name,
                                 x_max_priority=10,
                                 prefetch_count=1,
                                 bind_dead_queue=True)
        MQ_Manager.consume(queue_name=queue_name, prefetch_count=1,
                           on_message_callback=consume_callback)
        # MQ_RECEIVER.start()
        while 1:
            try:
                listener.start()  # 启动发送线程
                MQ_Manager.start_consume()
            except Exception as e:
                LOGGER.other_op.warning("检测到rabbit mq 链接丢失，尝试重新链接")
    except Exception as e:
        LOGGER.other_op.warning(f"消息队列连接失败！ - {e}")


if __name__ == '__main__':
    data_stored()
