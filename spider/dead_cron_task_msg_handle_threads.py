#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Project ：  spider-nav-config-platform2 
@File    :   dead_cron_task_msg_handle_threads.py
@Time    :   2024/5/7 10:38
@Author  :   MengJia_Fang 501738
@E-mail  :   fangmj@finchina.com
@License :   (C)Copyright Financial China Information & Technology Co., Ltd.
@Desc    :   Description
@Version :   1.0
"""

import datetime
import json
import os
import sys
import traceback

parent_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(parent_path)
from utils.jz_utils import check_msg_is_expire, get_dead_msg_config_from_toml
from utils.tools import OhMyExtractor
from spider_logging.kafka_logging import start_hear_beat, listener
from spider_logging.kafkalogger import KafkaLogger
import utils.env
import utils.settings
from utils.mqUtil import RabbitMqManager
from utils.spider_constant import DEAD_TEST_LC_JZ_CRON_TASK_QUEUE_NAME, \
    DEAD_LC_JZ_CRON_TASK_QUEUE_NAME, DEAD_CRON_TASK_FLAG

LOG_APP_ID = ''
LOGGER_NAME = 'spider.handle_msg_from_cron_task_dead_queue'
LOGGER = KafkaLogger(name=LOGGER_NAME, appid=LOG_APP_ID)
start_hear_beat(print_console=True, app_id=LOG_APP_ID)

# 数据库环境 test/formal
db_env = utils.env.DB_ENV
# 消息队列环境 test/formal
mq_env = utils.env.MQ_ENV

# # mongodb环境 test/formal
# mongo_env = utils.env.MONGO_ENV

# OPSQL = OhMyDb(db_env)
#
# MONGO = pymongo.MongoClient(utils.settings.MONGO_INFO[mongo_env]['CLIENT'])[
#     utils.settings.MONGO_INFO[mongo_env]['DB']][
#     utils.settings.MONGO_INFO[mongo_env]['COL_DATA_NEW']]
# path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
# sys.path.append(path)

plugin_path = os.path.join(
    os.path.abspath(os.path.dirname(os.path.abspath(__file__)) + os.path.sep), 'plugins')
sys.path.append(plugin_path)
MQ_Manager = ''


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
        queue_name = body_dict.get('send_to_next_mq', '')
        sourceid = body_dict.get("sourceid", "")
        msg_create_time = body_dict.get('msg_create_time', '')
        if not (sourceid and queue_name):
            # 此消息无法处理，直接忽略
            LOGGER.other_op.info(
                f'''配置表sourceid:{sourceid} - {DEAD_CRON_TASK_FLAG} - 网址：{body_dict.get("_url", "")} - {body_dict} - 报错：获取到的当前消息中的sourceid或send_to_next_mq参数为空！''')
            ch.basic_ack(delivery_tag=method.delivery_tag)
            LOGGER.receive_message.info(f"00 - [tag] - {method.delivery_tag} -消息已确认消费")
            return

        # 先获取到需要重新入到原始队列的信源列表，若当前信源在信源列表中，则当前信源消息重新发往原始队列;
        # 若当前信源不在信源列表中暂时不处理，还是入当前队列，判断消息距离当前时间过期的话就直接丢弃
        if db_env == '185':
            config_key = DEAD_TEST_LC_JZ_CRON_TASK_QUEUE_NAME
        elif db_env == 'formal':
            config_key = DEAD_LC_JZ_CRON_TASK_QUEUE_NAME
        else:
            raise Exception(f'未知的运行环境：{db_env}')
        source_id_list, _ttl_for_source_id_list, dead_msg_ttl = get_dead_msg_config_from_toml(
            config_key)

        if sourceid not in source_id_list:
            # 判断消息是否过期
            _ttl = ''
            for ttl_source in _ttl_for_source_id_list:
                if ttl_source[0] == sourceid:
                    _ttl = ttl_source[1]
                    break
            else:
                _ttl = dead_msg_ttl
            # if not _ttl:
            #     LOGGER.other_op.info(
            #         f'''配置表sourceid:{sourceid} - {DEAD_CRON_TASK_FLAG} - 网址：{body_dict.get("_url", "")} - {body_dict} - 报错：获取到的当前死信队列的消息过期时间为空，无法处理，重新入回死信队列！''')
            #     ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            #     return

            ttl_discard_flag = check_msg_is_expire(msg_create_time, _ttl)
            if ttl_discard_flag:
                LOGGER.other_op.info(
                    f'''配置表sourceid:{sourceid} - {DEAD_CRON_TASK_FLAG} - 网址：{body_dict.get("_url", "")} - {body_dict} - 当前消息过期了,丢弃！''')
                ch.basic_ack(delivery_tag=method.delivery_tag)
                LOGGER.receive_message.info(f"00 - [tag] - {method.delivery_tag} -消息已确认消费")
                return
            else:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                return
        try:
            body_dict['program_flag'] = DEAD_CRON_TASK_FLAG
            body_dict['msg_create_time'] = datetime.datetime.now().strftime(
                '%Y-%m-%d %H:%M:%S')
            second_msg = json.dumps(body_dict, ensure_ascii=False)
            global MQ_Manager
            if not MQ_Manager:
                MQ_Manager = RabbitMqManager(mq_env)
            MQ_Manager.queue_declare(queue_name=queue_name, prefetch_count=1,
                                     bind_dead_queue=True)
            MQ_Manager.send_json_msg(queue_name=queue_name, json_str=second_msg)
            LOGGER.other_op.info(
                f'''配置表sourceid:{sourceid} - {DEAD_CRON_TASK_FLAG} - 网址：{body_dict.get("_url", "")} - {body_dict} - 消息推送至{queue_name}成功！''')
        except Exception as e:
            LOGGER.other_op.info(
                f'''配置表sourceid:{sourceid} - {DEAD_CRON_TASK_FLAG} - 网址：{body_dict.get("_url", "")} - {body_dict} - 消息推送至{queue_name}失败！- {str(traceback.format_exc())}''')
            raise Exception(
                f'''配置表sourceid:{sourceid} - {DEAD_CRON_TASK_FLAG} - 网址：{body_dict.get("_url", "")} - {body_dict} - 消息推送至{queue_name}失败！- {str(traceback.format_exc())}''')
    except Exception as e:
        LOGGER.other_op.warning(f'消息消费失败：{str(traceback.format_exc())},消息体：{body_temp},重新塞入死信队列')
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    else:
        ch.basic_ack(delivery_tag=method.delivery_tag)
        LOGGER.receive_message.info(f"00 - [tag] - {method.delivery_tag} -消息已确认消费")


def dlx_msg_handler():
    listener.start()  # 启动发送线程
    mac_ip = OhMyExtractor.get_ip()
    assert mac_ip
    LOGGER.other_op.debug(f'00 - 当前macip:{mac_ip}')
    LOGGER.other_op.info("00 - 死信队列 - 调度任务 - 消息处理程序启动!")

    if db_env == '185':
        queue_name = DEAD_TEST_LC_JZ_CRON_TASK_QUEUE_NAME
    elif db_env == 'formal':
        queue_name = DEAD_LC_JZ_CRON_TASK_QUEUE_NAME
    else:
        raise Exception(f'未知的运行环境：{db_env}')

    try:
        global MQ_Manager
        if not MQ_Manager:
            MQ_Manager = RabbitMqManager(mq_env)
        MQ_Manager.queue_declare(queue_name=queue_name, prefetch_count=1)
        MQ_Manager.consume(queue_name=queue_name, prefetch_count=1,
                           on_message_callback=consume_callback)
        while 1:
            try:
                listener.start()  # 启动发送线程
                MQ_Manager.start_consume()
            except Exception as e:
                LOGGER.other_op.warning("检测到rabbit mq 链接丢失，尝试重新链接")
    except Exception as e:
        LOGGER.other_op.warning(f"消息队列连接失败！ - {str(traceback.format_exc())}")


if __name__ == '__main__':
    dlx_msg_handler()
