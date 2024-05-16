#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Project ：  spider-nav-config-platform 
@File    :   data_deduplicate_threads.py
@Time    :   2024/3/20 13:44
@Author  :   MengJia_Fang 501738
@E-mail  :   fangmj@finchina.com
@License :   (C)Copyright Financial China Information & Technology Co., Ltd.
@Desc    :   Description
@Version :   1.0
"""
import datetime
import json
import os
import random
import sys

parent_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(parent_path)

from utils.tablename import get_download_table
import traceback
import pymongo
from spider_logging.kafka_logging import start_hear_beat, listener
from spider_logging.kafkalogger import KafkaLogger
import utils.env
import utils.settings
from utils.mqUtil import RabbitMqManager
from utils.spider_constant import LC_JZ_DATA_TO_BE_DEDUPLICATED_QUEUE_NAME, \
    TEST_LC_JZ_DATA_TO_BE_DEDUPLICATED_QUEUE_NAME, TEST_LC_JZ_DATA_TO_BE_STORED_QUEUE_NAME, \
    LC_JZ_DATA_TO_BE_STORED_QUEUE_NAME, DATA_DEDUPLICATE_FLAG
from utils.tools import OhMyExtractor

LOG_APP_ID = 'cdd8206606aa475e99c7d6fda38a449b'
LOGGER_NAME = 'spider.data_deduplicate' + "_" + str(random.randint(1, 1000))
LOGGER = KafkaLogger(name=LOGGER_NAME, appid=LOG_APP_ID)
start_hear_beat(print_console=True, app_id=LOG_APP_ID)

# 数据库环境 test/formal
db_env = utils.env.DB_ENV
# 消息队列环境 test/formal
mq_env = utils.env.MQ_ENV

# mongodb环境 test/formal
mongo_env = utils.env.MONGO_ENV

# path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
# sys.path.append(path)
# plugin_path = os.path.join(
#     os.path.abspath(os.path.dirname(os.path.abspath(__file__)) + os.path.sep), 'plugins')
# sys.path.append(plugin_path)

MQ_Manager = ''
mongo_1 = pymongo.MongoClient(utils.settings.MONGO_INFO[mongo_env]['CLIENT'])[
    utils.settings.MONGO_INFO[mongo_env]['DB']][
    utils.settings.MONGO_INFO[mongo_env][f'COL_DATA_MODULE_{get_download_table(1).split(",")[0]}']]
mongo_2 = pymongo.MongoClient(utils.settings.MONGO_INFO[mongo_env]['CLIENT'])[
    utils.settings.MONGO_INFO[mongo_env]['DB']][
    utils.settings.MONGO_INFO[mongo_env][f'COL_DATA_MODULE_{get_download_table(2).split(",")[0]}']]
mongo_3 = pymongo.MongoClient(utils.settings.MONGO_INFO[mongo_env]['CLIENT'])[
    utils.settings.MONGO_INFO[mongo_env]['DB']][
    utils.settings.MONGO_INFO[mongo_env][f'COL_DATA_MODULE_{get_download_table(3).split(",")[0]}']]
mongo_4 = pymongo.MongoClient(utils.settings.MONGO_INFO[mongo_env]['CLIENT'])[
    utils.settings.MONGO_INFO[mongo_env]['DB']][
    utils.settings.MONGO_INFO[mongo_env][f'COL_DATA_MODULE_{get_download_table(4).split(",")[0]}']]
mongo_5 = pymongo.MongoClient(utils.settings.MONGO_INFO[mongo_env]['CLIENT'])[
    utils.settings.MONGO_INFO[mongo_env]['DB']][
    utils.settings.MONGO_INFO[mongo_env][f'COL_DATA_MODULE_{get_download_table(5).split(",")[0]}']]
mongo_6 = pymongo.MongoClient(utils.settings.MONGO_INFO[mongo_env]['CLIENT'])[
    utils.settings.MONGO_INFO[mongo_env]['DB']][
    utils.settings.MONGO_INFO[mongo_env][f'COL_DATA_MODULE_{get_download_table(6).split(",")[0]}']]
mongo_7 = pymongo.MongoClient(utils.settings.MONGO_INFO[mongo_env]['CLIENT'])[
    utils.settings.MONGO_INFO[mongo_env]['DB']][
    utils.settings.MONGO_INFO[mongo_env][f'COL_DATA_MODULE_{get_download_table(7).split(",")[0]}']]
mongo_8 = pymongo.MongoClient(utils.settings.MONGO_INFO[mongo_env]['CLIENT'])[
    utils.settings.MONGO_INFO[mongo_env]['DB']][
    utils.settings.MONGO_INFO[mongo_env][f'COL_DATA_MODULE_{get_download_table(8).split(",")[0]}']]
mongo_9 = pymongo.MongoClient(utils.settings.MONGO_INFO[mongo_env]['CLIENT'])[
    utils.settings.MONGO_INFO[mongo_env]['DB']][
    utils.settings.MONGO_INFO[mongo_env][f'COL_DATA_MODULE_{get_download_table(9).split(",")[0]}']]
mongo_10 = pymongo.MongoClient(utils.settings.MONGO_INFO[mongo_env]['CLIENT'])[
    utils.settings.MONGO_INFO[mongo_env]['DB']][
    utils.settings.MONGO_INFO[mongo_env][f'COL_DATA_MODULE_{get_download_table(10).split(",")[0]}']]
mongo_11 = pymongo.MongoClient(utils.settings.MONGO_INFO[mongo_env]['CLIENT'])[
    utils.settings.MONGO_INFO[mongo_env]['DB']][
    utils.settings.MONGO_INFO[mongo_env][f'COL_DATA_MODULE_{get_download_table(11).split(",")[0]}']]
mongo_12 = pymongo.MongoClient(utils.settings.MONGO_INFO[mongo_env]['CLIENT'])[
    utils.settings.MONGO_INFO[mongo_env]['DB']][
    utils.settings.MONGO_INFO[mongo_env][f'COL_DATA_MODULE_{get_download_table(12).split(",")[0]}']]
mongo_13 = pymongo.MongoClient(utils.settings.MONGO_INFO[mongo_env]['CLIENT'])[
    utils.settings.MONGO_INFO[mongo_env]['DB']][
    utils.settings.MONGO_INFO[mongo_env][f'COL_DATA_MODULE_{get_download_table(13).split(",")[0]}']]
mongo_14 = pymongo.MongoClient(utils.settings.MONGO_INFO[mongo_env]['CLIENT'])[
    utils.settings.MONGO_INFO[mongo_env]['DB']][
    utils.settings.MONGO_INFO[mongo_env][f'COL_DATA_MODULE_{get_download_table(14).split(",")[0]}']]
mongo_15 = pymongo.MongoClient(utils.settings.MONGO_INFO[mongo_env]['CLIENT'])[
    utils.settings.MONGO_INFO[mongo_env]['DB']][
    utils.settings.MONGO_INFO[mongo_env][f'COL_DATA_MODULE_{get_download_table(15).split(",")[0]}']]
mongo_16 = pymongo.MongoClient(utils.settings.MONGO_INFO[mongo_env]['CLIENT'])[
    utils.settings.MONGO_INFO[mongo_env]['DB']][
    utils.settings.MONGO_INFO[mongo_env][f'COL_DATA_MODULE_{get_download_table(16).split(",")[0]}']]


class DataDeDuplicate:
    def __init__(self, extractor):
        self.OhMyExtractor = extractor

    def run(self, record):
        global MQ_Manager
        if not record.get('grabdatacode', '') or record['grabdatacode'] == 'None':
            LOGGER.set_data_source_id('0', durable=True)
        else:
            LOGGER.set_data_source_id(record['grabdatacode'], durable=True)

        originTime1 = datetime.datetime.now().strftime("%Y-%m-%d")
        originTime2 = datetime.datetime.now().strftime("%H:%M:%S")

        LOGGER.set_origin_msg_time(f'{originTime1} {originTime2}.000')
        sourceid = record['sourceid']
        dup_range = record.get('dup_range', '')
        # 判断是不是多表
        multi_guid_rule = record.get('multi_guid_rule', '')
        sub_data_guid = record.get('sub_data_guid', '')
        stored_field = record['stored_fields']
        data_guid = record.get('data_guid', '')
        send_to_download_flag = False
        # 控制子表下载表是否入库
        send_to_sub_download_flag = False
        send_to_mongo_flag = False
        log_msg = ''
        if not data_guid:
            LOGGER.de_duplication.warning(
                f'配置表sourceid:{sourceid} - 网址：{record.get("_url", "")} - 数据guid:{data_guid} - 去重失败：未获取到数据对应的guid')
            raise Exception(
                f'配置表sourceid:{sourceid} - 网址：{record.get("_url", "")} - 数据guid:{data_guid} - 去重失败：未获取到数据对应的guid!')
        if not record.get('modulecode', ''):
            LOGGER.de_duplication.warning(
                f'配置表sourceid:{sourceid} - 网址：{record.get("_url", "")} - 数据guid:{data_guid} - 去重失败：单表去重时，未获取到数据对应的modulecode!')
            raise Exception(
                f'配置表sourceid:{sourceid} - 网址：{record.get("_url", "")} - 数据guid:{data_guid} - 去重失败：单表去重时，未获取到数据对应的modulecode!')

        mongo = self.get_mongo_client(record['modulecode'])

        if multi_guid_rule:
            # 是多表数据
            if not dup_range:
                # 有多表的情况:唯一索引不能限制主表数据插入，需要先查询
                # 无需去重的,主表和从表的下载表都要入，mongodb要判断下
                send_to_download_flag = True
                send_to_sub_download_flag = True
                log_msg = '- 不需要去重的数据'
                if record.get('modulecode', None) and record['modulecode'] in (15, 16):
                    # 主表去重
                    dup_dict = {"sourceid": sourceid, "data_guid": data_guid}
                    if sub_data_guid:
                        dup_dict["sub_data_guid"] = sub_data_guid
                    existed = mongo.find_one(dup_dict)
                    # 子表数据存在的话，主表一定也存在
                    if not existed:
                        send_to_mongo_flag = True
                else:
                    raise Exception(
                        f'配置表sourceid:{sourceid} - 网址：{record.get("_url", "")} - 数据guid:{data_guid} - 去重失败：多表去重模块暂不支持当前模块{record.get("modulecode", "")}!')
            else:
                dup_range = dup_range.replace('，', ',')
                dup_range_list = dup_range.split(',')

                # 主表去重
                dup_dict = {}
                # if 'table' in dup_range_list:
                #     dup_dict["modulecode"] = record['modulecode']
                if 'sourceid' in dup_range_list:
                    dup_dict["sourceid"] = sourceid
                if 'data_guid' in dup_range_list:
                    dup_dict["data_guid"] = data_guid
                existed = mongo.find_one(dup_dict)
                if existed:
                    # 主表存在
                    LOGGER.de_duplication.info(
                        f'配置表sourceid:{sourceid} - 网址：{record.get("_url", "")} - 数据guid:{data_guid} - 数据从表guid:{sub_data_guid} - MongoDB重复,无需入库!')
                    if sub_data_guid:
                        dup_dict["sub_data_guid"] = sub_data_guid
                        sub_existed = mongo.find_one(dup_dict)
                        if not sub_existed:
                            send_to_sub_download_flag = True
                            send_to_mongo_flag = True
                else:
                    # 主表需要入库,子表必然要入库
                    send_to_download_flag = True
                    send_to_sub_download_flag = True
                    send_to_mongo_flag = True

            if send_to_mongo_flag:
                try:
                    insert_dict = {
                        "data_guid": data_guid,
                        "sourceid": sourceid,
                        "modulecode": record['modulecode']
                    }
                    if sub_data_guid:
                        insert_dict["sub_data_guid"] = sub_data_guid
                    mongo.insert_one(insert_dict)
                except Exception as e:
                    if 'duplicate key' in str(e):
                        if dup_range:
                            LOGGER.insert_mongo.info(
                                f'配置表sourceid:{sourceid} - {DATA_DEDUPLICATE_FLAG} - {stored_field} - 下载表guid:{data_guid} {log_msg}- 入库MongoDB失败 - 数据重复,无需入库下载表!')
                            return
                            # else:
                        #     LOGGER.insert_mongo.info(
                        #         f'配置表sourceid:{sourceid} - {DATA_DEDUPLICATE_FLAG} - {stored_field} - 下载表guid:{data_guid} {log_msg}- 入库MongoDB失败 - 数据重复,继续入库下载表!')
                    else:
                        # LOGGER.insert_mongo.error(
                        #     f'配置表sourceid:{sourceid} - {DATA_DEDUPLICATE_FLAG} - {stored_field} - 下载表guid:{data_guid} {log_msg}- 入库MongoDB失败 - {str(traceback.format_exc())}')
                        raise Exception(
                            f'配置表sourceid:{sourceid} - {DATA_DEDUPLICATE_FLAG} - {stored_field} - 下载表guid:{data_guid} {log_msg}- 入库MongoDB失败 - {str(traceback.format_exc())}')
                else:
                    LOGGER.insert_mongo.info(
                        f'配置表sourceid:{sourceid} - {DATA_DEDUPLICATE_FLAG} - {stored_field} - 下载表guid:{data_guid} {log_msg}- 入库MongoDB成功!')

            if db_env == '185':
                queue_name = TEST_LC_JZ_DATA_TO_BE_STORED_QUEUE_NAME
            elif db_env == 'formal':
                queue_name = LC_JZ_DATA_TO_BE_STORED_QUEUE_NAME
            else:
                raise Exception(
                    f'配置表sourceid:{sourceid} - 网址：{record["_url"]} - {stored_field} - 去重报错：未知的运行环境：{db_env}')
            if not MQ_Manager:
                MQ_Manager = RabbitMqManager(mq_env)
            MQ_Manager.queue_declare(queue_name=queue_name, x_max_priority=10,
                                     prefetch_count=1,
                                     bind_dead_queue=True)
            if send_to_download_flag:
                LOGGER.de_duplication.info(
                    f'配置表sourceid:{sourceid} - 网址：{record.get("_url", "")} - 数据guid:{data_guid} {log_msg}- 需要入库下载表!')
                try:
                    # sub_data_guid为空的数据只入主表，不入子表
                    record['sub_data_guid'] = ''
                    record['program_flag'] = DATA_DEDUPLICATE_FLAG
                    record['send_to_next_mq'] = queue_name
                    record['msg_create_time'] = datetime.datetime.now().strftime(
                        '%Y-%m-%d %H:%M:%S')
                    second_msg = json.dumps(record, ensure_ascii=False)
                    # send = OhMyMqSender(queue_name=queue_name,
                    #                     priority=15,
                    #                     mq_env=mq_env)  # 历史数据消息等级2 ，日常消息等级15
                    MQ_Manager.send_json_msg(queue_name=queue_name, json_str=second_msg,
                                             priority=record.get('msg_priority', 0))
                    LOGGER.other_op.info(
                        f'''配置表sourceid:{sourceid} - 网址：{record.get("_url", "")} - {stored_field} - 消息推送至{queue_name}成功！''')
                except Exception as e:
                    LOGGER.other_op.info(
                        f'''配置表sourceid:{sourceid} - 网址：{record.get("_url", "")} - {stored_field} - 消息推送至{queue_name}失败！- {e}''')
                    raise Exception(
                        f'''配置表sourceid:{sourceid} - 网址：{record.get("_url", "")} - {stored_field} - 消息推送至{queue_name}失败！- {e}''')

            if send_to_sub_download_flag and sub_data_guid:
                # sub_data_guid不为空的只入子表
                LOGGER.de_duplication.info(
                    f'配置表sourceid:{sourceid} - 网址：{record.get("_url", "")} - 数据guid:{data_guid} - 从表数据sub_data_guid{sub_data_guid}{log_msg}- 需要入库下载表!')
                try:
                    record['program_flag'] = DATA_DEDUPLICATE_FLAG
                    record['send_to_next_mq'] = queue_name
                    record['msg_create_time'] = datetime.datetime.now().strftime(
                        '%Y-%m-%d %H:%M:%S')
                    second_msg = json.dumps(record, ensure_ascii=False)
                    MQ_Manager.send_json_msg(queue_name=queue_name, json_str=second_msg,
                                             priority=record.get('msg_priority', 0))
                    LOGGER.other_op.info(
                        f'''配置表sourceid:{sourceid} - 网址：{record.get("_url", "")} - 数据guid:{data_guid} - 从表数据sub_data_guid{sub_data_guid} - {stored_field} - 消息推送至{queue_name}成功！''')
                except Exception as e:
                    # LOGGER.other_op.info(
                    #     f'''配置表sourceid:{sourceid} - 网址：{record.get("_url", "")} - {stored_field} - 消息推送至{queue_name}失败！- {e}''')
                    raise Exception(
                        f'''配置表sourceid:{sourceid} - 网址：{record.get("_url", "")} - 数据guid:{data_guid} - 从表数据sub_data_guid{sub_data_guid} - {stored_field} - 消息推送至{queue_name}失败！- {str(traceback.format_exc())}''')

        else:
            # 单下载表数据
            if not dup_range:
                # 不需要去重的也在这处理 不报错,直接插入mongodb，mongodb有加唯一索引的话重复的插不进去
                # 没加唯一索引的话，需要先查询，不存在再插入
                send_to_download_flag = True
                log_msg = '- 不需要去重的数据'
            else:
                dup_range = dup_range.replace('，', ',')
                dup_range_list = dup_range.split(',')

                # 主表去重
                dup_dict = {}
                # if 'table' in dup_range_list:
                #     dup_dict["modulecode"] = record['modulecode']
                if 'sourceid' in dup_range_list:
                    dup_dict["sourceid"] = sourceid
                if 'data_guid' in dup_range_list:
                    dup_dict["data_guid"] = data_guid

                existed = mongo.find_one(dup_dict)
                if existed:
                    LOGGER.de_duplication.info(
                        f'配置表sourceid:{sourceid} - 网址：{record.get("_url", "")} - 数据guid:{data_guid} - MongoDB重复,无需入库!')
                    # return
                else:
                    send_to_download_flag = True

            if send_to_download_flag:
                try:
                    mongo.insert_one({
                        "data_guid": data_guid,
                        "sourceid": sourceid,
                        "modulecode": record['modulecode']
                    })
                except Exception as e:
                    if 'duplicate key' in str(e):
                        if dup_range:
                            LOGGER.insert_mongo.info(
                                f'配置表sourceid:{sourceid} - {DATA_DEDUPLICATE_FLAG} - {stored_field} - 下载表guid:{data_guid} {log_msg}- 入库MongoDB失败 - 数据重复,无需入库下载表!')
                            return
                            # else:
                        #     LOGGER.insert_mongo.info(
                        #         f'配置表sourceid:{sourceid} - {DATA_DEDUPLICATE_FLAG} - {stored_field} - 下载表guid:{data_guid} {log_msg}- 入库MongoDB失败 - 数据重复,继续入库下载表!')
                    else:
                        LOGGER.insert_mongo.error(
                            f'配置表sourceid:{sourceid} - {DATA_DEDUPLICATE_FLAG} - {stored_field} - 下载表guid:{data_guid} {log_msg}- 入库MongoDB失败 - {str(traceback.format_exc())}')
                        raise Exception(
                            f'配置表sourceid:{sourceid} - {DATA_DEDUPLICATE_FLAG} - {stored_field} - 下载表guid:{data_guid} {log_msg}- 入库MongoDB失败 - {str(traceback.format_exc())}')
                else:
                    LOGGER.insert_mongo.info(
                        f'配置表sourceid:{sourceid} - {DATA_DEDUPLICATE_FLAG} - {stored_field} - 下载表guid:{data_guid} {log_msg}- 入库MongoDB成功!')
                    LOGGER.de_duplication.info(
                        f'配置表sourceid:{sourceid} - 网址：{record.get("_url", "")} - 数据guid:{data_guid} {log_msg}- 需要入库下载表!')

                if db_env == '185':
                    queue_name = TEST_LC_JZ_DATA_TO_BE_STORED_QUEUE_NAME
                elif db_env == 'formal':
                    queue_name = LC_JZ_DATA_TO_BE_STORED_QUEUE_NAME
                else:
                    raise Exception(
                        f'配置表sourceid:{sourceid} - 网址：{record["_url"]} - {stored_field} - 去重报错：未知的运行环境：{db_env}')

                try:
                    record['program_flag'] = DATA_DEDUPLICATE_FLAG
                    record['send_to_next_mq'] = queue_name
                    record['msg_create_time'] = datetime.datetime.now().strftime(
                        '%Y-%m-%d %H:%M:%S')
                    second_msg = json.dumps(record, ensure_ascii=False)
                    # send = OhMyMqSender(queue_name=queue_name,
                    #                     priority=15,
                    #                     mq_env=mq_env)  # 历史数据消息等级2 ，日常消息等级15
                    if not MQ_Manager:
                        MQ_Manager = RabbitMqManager(mq_env)
                    MQ_Manager.queue_declare(queue_name=queue_name, x_max_priority=10,
                                             prefetch_count=1,
                                             bind_dead_queue=True)
                    MQ_Manager.send_json_msg(queue_name=queue_name, json_str=second_msg,
                                             priority=record.get('msg_priority', 0))
                    LOGGER.other_op.info(
                        f'''配置表sourceid:{sourceid} - 网址：{record.get("_url", "")} - {stored_field} - 消息推送至{queue_name}成功！''')
                except Exception as e:
                    LOGGER.other_op.info(
                        f'''配置表sourceid:{sourceid} - 网址：{record.get("_url", "")} - {stored_field} - 消息推送至{queue_name}失败！- {e}''')
                    raise Exception(
                        f'''配置表sourceid:{sourceid} - 网址：{record.get("_url", "")} - {stored_field} - 消息推送至{queue_name}失败！- {e}''')

    @staticmethod
    def get_mongo_client(modulecode):
        if modulecode == 1:
            return mongo_1
        elif modulecode == 2:
            return mongo_2
        elif modulecode == 3:
            return mongo_3
        elif modulecode == 4:
            return mongo_4
        elif modulecode == 5:
            return mongo_5
        elif modulecode == 6:
            return mongo_6
        elif modulecode == 7:
            return mongo_7
        elif modulecode == 8:
            return mongo_8
        elif modulecode == 9:
            return mongo_9
        elif modulecode == 10:
            return mongo_10
        elif modulecode == 11:
            return mongo_11
        elif modulecode == 12:
            return mongo_12
        elif modulecode == 13:
            return mongo_13
        elif modulecode == 14:
            return mongo_14
        elif modulecode == 15:
            return mongo_15
        elif modulecode == 16:
            return mongo_16

    @staticmethod
    def create_mongo_client(mongo_env, modulecode):
        table_name = get_download_table(modulecode).split(",")[0]
        col_name = f'COL_DATA_MODULE_{table_name}'
        mongo = pymongo.MongoClient(utils.settings.MONGO_INFO[mongo_env]['CLIENT'])[
            utils.settings.MONGO_INFO[mongo_env]['DB']][
            utils.settings.MONGO_INFO[mongo_env][col_name]]
        return mongo


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
        deduplicate = DataDeDuplicate(OhMyExtractor)
        deduplicate.run(body_dict)
    except Exception as e:
        LOGGER.other_op.warning(f'消息消费失败：{str(traceback.format_exc())},消息体：{body_temp}')
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    else:
        ch.basic_ack(delivery_tag=method.delivery_tag)
        LOGGER.receive_message.info(f"00 - [tag]{method.delivery_tag} -消息已确认消费")


def data_deduplicate():
    listener.start()  # 启动发送线程
    macip = OhMyExtractor.get_ip()
    assert macip
    LOGGER.other_op.debug(f'00 - 当前macip:{macip}')
    LOGGER.other_op.info("00 - 数据去重程序启动!")

    # pool = ThreadPoolExecutor(max_workers=10)
    # record_queue = queue.Queue(maxsize=10)

    if db_env == 'formal':
        queue_name = LC_JZ_DATA_TO_BE_DEDUPLICATED_QUEUE_NAME
    elif db_env == '185':
        queue_name = TEST_LC_JZ_DATA_TO_BE_DEDUPLICATED_QUEUE_NAME
    else:
        raise Exception(f'未知的运行环境：{db_env}')

    try:
        global MQ_Manager
        if not MQ_Manager:
            MQ_Manager = RabbitMqManager(mq_env)
        MQ_Manager.queue_declare(queue_name=queue_name, x_max_priority=10, prefetch_count=1,
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


def query_guid_existed_mongo(item):
    modulecode = item.get("modulecode", None)
    sourceid = item.get("sourceid", None)
    data_guid = item.get("guid_str", None)
    if not sourceid:
        raise Exception(
            f'手动查询guid是否存在mongodb中 - 报错：sourceid不能为空!')
    if not modulecode:
        raise Exception(
            f'手动查询guid是否存在mongodb中 - 报错：modulecode不能为空!')
    if not data_guid:
        raise Exception(
            f'手动查询guid是否存在mongodb中 - 报错：要查询的guid不能为空!')
    mongo1 = DataDeDuplicate.get_mongo_client(int(modulecode))
    existed = mongo1.find_one({"data_guid": data_guid})
    if existed:
        LOGGER.other_op.debug(
            f'配置表sourceid:{sourceid}- 手动查询guid是否存在mongodb中 - 模块编码：{modulecode} - 数据guid:{data_guid} - data_guid已存在!')
        return '存在'
    else:
        LOGGER.other_op.debug(
            f'配置表sourceid:{sourceid}- 手动查询guid是否存在mongodb中 - 模块编码：{modulecode} - 数据guid:{data_guid} - data_guid不存在!')
        return '不存在'


if __name__ == '__main__':
    data_deduplicate()
