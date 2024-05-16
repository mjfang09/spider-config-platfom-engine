#!/usr/bin/env python
# -*- encoding: utf-8 -*-
"""
@Project ：  spider-nav-config-platform
@File    :   data_download_threads.py
@Time    :   2024/03/13 14:40
@Author  :   MengJia_Fang 501738
@E-mail  :   fangmj@finchina.com
@License :   (C)Copyright Financial China Information & Technology Co., Ltd.
@Desc    :   框架抓取数据的网页下载步骤
@Version :   1.0
"""

# here put the import lib
import datetime
import json
import os
import pathlib
import random
import re
import sys
import traceback

parent_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(parent_path)

from enum import Enum
from spider_logging.kafka_logging import listener, start_hear_beat
from spider_logging.kafkalogger import KafkaLogger

import utils.env
import utils.settings
from utils.dbUtil import OhMyDb
from utils.jz_utils import get_cookie_from_file, import_module_from_str, get_file_path_from_module
from utils.mqUtil import RabbitMqManager
from utils.reqUtil import OhMyReq
from utils.spider_constant import TEST_LC_JZ_DATA_TO_BE_DOWNLOADED_QUEUE_NAME, \
    LC_JZ_DATA_TO_BE_DOWNLOADED_QUEUE_NAME, \
    TEST_LC_JZ_DATA_TO_BE_EXTRACT_QUEUE_NAME, LC_JZ_DATA_TO_BE_EXTRACT_QUEUE_NAME, \
    DATA_DOWNLOAD_FLAG, SAVE_SOURCE_CODE_FILE_PATH
from utils.tools import OhMyExtractor

LOG_APP_ID = '574c33cd194e4fb0abd13d7a3710b76e'
LOGGER_NAME = 'spider.data_download' + "_" + str(random.randint(1, 1000))
LOGGER = KafkaLogger(name=LOGGER_NAME, appid=LOG_APP_ID)
start_hear_beat(print_console=True, app_id=LOG_APP_ID)

# 数据库环境 test/formal
db_env = utils.env.DB_ENV
# 消息队列环境 test/formal
mq_env = utils.env.MQ_ENV

# # mongodb环境 test/formal
# mongo_env = utils.env.MONGO_ENV

REQ = OhMyReq()
OPSQL = OhMyDb(db_env)
# FILE_ROOT = utils.settings.FILE_INFO[file_env]

# MONGO = pymongo.MongoClient(utils.settings.MONGO_INFO[mongo_env]['CLIENT'])[
#     utils.settings.MONGO_INFO[mongo_env]['DB']][utils.settings.MONGO_INFO[mongo_env]['COL_DATA']]
path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(path)
plugin_path = os.path.join(
    os.path.abspath(os.path.dirname(os.path.abspath(__file__)) + os.path.sep), 'plugins')
sys.path.append(plugin_path)
MQ_Manager = ''


class ProcessState(Enum):
    REQUEST = -1
    RULE_EXECUTE = -2
    SUCCESS = 0


class DataDownloader:
    def __init__(self, req):
        self.REQ = req

    def run(self, record):
        global db_env
        if not record.get('grabdatacode', '') or record['grabdatacode'] == 'None':
            LOGGER.set_data_source_id('0', durable=True)
        else:
            LOGGER.set_data_source_id(record['grabdatacode'], durable=True)

        originTime1 = datetime.datetime.now().strftime("%Y-%m-%d")
        originTime2 = datetime.datetime.now().strftime("%H:%M:%S")

        LOGGER.set_origin_msg_time(f'{originTime1} {originTime2}.000')

        post_data = ''
        sourceid = record['sourceid']
        current_layer = record['current_layer']
        total_layers = record['total_layers']
        task_type = record.get('task_type', '')
        url = record['_url'].replace('&amp;', '&')
        url = url.replace(r'\u0028', '(')
        url = url.replace(r'\u0029', ')')

        if current_layer == 1 or total_layers == 1:
            # 第一级网址的抓取配置从主配置表字段取
            if record['isjson']:
                record['isjson'] = record['isjson'].split(',')[0]
            # record['field_rule'] = record['layer_0_field_rule']
            record['plugin'] = record['layer_0_plugin']
            if record['header']:
                db_header = json.loads(record['header'])
                record['header'] = db_header.get('list_header', '')
            # if record.get('postdata',''):
            #     db_postdata = json.loads(record['postdata'])
            #     if db_postdata.get("list_post", None):
            #         post_data = db_postdata['list_post']
            db_charset = record.get('textencoding', '')
            if db_charset and ',' in db_charset:
                record['textencoding'] = db_charset.split(',')[0]
        elif current_layer == total_layers:
            # 正文网址的抓取配置从主配置表字段取
            if record['isjson']:
                record['isjson'] = record['isjson'].split(',')[1]
            # record['field_rule'] = record['conrule']
            record['plugin'] = record['content_plugin']
            if record['header']:
                db_header = json.loads(record['header'])
                record['header'] = db_header.get('content_header', '')
            db_charset = record.get('textencoding', '')
            if db_charset and ',' in db_charset:
                record['textencoding'] = db_charset.split(',')[1]
        else:
            record['isjson'] = record['is_json']
            record['plugin'] = record['plugin']
            record['header'] = json.loads(record['header']) if record['header'] else ''
            # headers = record['header']
        if task_type and task_type == 'test':
            all_field_obj = record
        else:
            # 从消息体中获取之前层级的中间字段
            all_mid_layer_data = record.get('all_layer_data', '')
            all_field_obj, all_field_name = self.get_all_field_name_and_value(all_mid_layer_data)
        headers = ''
        if record['header']:
            headers = record['header']
            if re.findall('\=\>(.+?)\<\=', headers):
                headers_temp = headers
                h_other_field_name_list = re.findall('\=\>(.+?)\<\=', headers_temp)
                for h_other_field_name in h_other_field_name_list:
                    if all_field_obj.get(h_other_field_name, None) is None:
                        raise Exception(
                            f'配置表sourceid：{sourceid} - {DATA_DOWNLOAD_FLAG} - {task_type} - 请求头中引用的字段值{h_other_field_name}不存在，请检查-引用其他字段的顺序，被引用字段必须先配置')

                    headers_temp = re.sub(fr'\=\>{h_other_field_name}\<\=',
                                          all_field_obj[h_other_field_name],
                                          headers_temp)

                headers = headers_temp
            cookie_paths = re.findall('#cookie_path=(.+?)#', headers.replace('\n', ','),
                                      re.I)
            if cookie_paths:
                cookie = get_cookie_from_file(cookie_paths[0], LOGGER_NAME, LOG_APP_ID)
                headers = re.sub('#cookie_path=.*#\n', cookie + '\n', headers, re.I)

        if '#post#=' in url:
            urla, posta = url.split('#post#=')
            if re.findall('\=\>(.+?)\<\=', posta):
                post_temp = posta
                h_other_field_name_list1 = re.findall('\=\>(.+?)\<\=', post_temp)
                for h_other_field_name1 in h_other_field_name_list1:
                    if all_field_obj.get(h_other_field_name1, None) is None:
                        raise Exception(
                            f'配置表sourceid：{sourceid} - {DATA_DOWNLOAD_FLAG} - {task_type} - post参数中引用的字段值{h_other_field_name1}不存在，请检查-引用其他字段的顺序，被引用字段必须先配置')

                    post_temp = re.sub(fr'\=\>{h_other_field_name1}\<\=',
                                       all_field_obj[h_other_field_name1],
                                       post_temp)
                    posta = post_temp
            url = (urla, posta)
        else:
            # 目前传进来的url入过有post参数的都直接拼接了
            pass

        # 配置表数据
        charset = record['textencoding']
        proxies = ['']
        if record['useproxy'] == 1:
            if not record['proxys']:
                proxies = ['10.17.205.91:808', '10.17.205.96:808', '10.17.206.27:808',
                           '10.17.206.29:808', '10.17.206.28:808']
            else:
                proxies = record['proxys']
                if ',' in proxies:
                    proxies = proxies.split(',')
                else:
                    proxies = [record['proxys']]

        # 请求源码
        error_flag = 0
        resource = ''
        if 1 == 1:
            # 导入插件模块 - START
            import_current_layer_module = None
            import_module_name = f'{sourceid}_{current_layer}_{DATA_DOWNLOAD_FLAG}'
            if record.get('plugin', ''):
                import_current_layer_module = import_module_from_str(
                    import_module_name,
                    record['plugin'],
                    log_name=LOGGER_NAME,
                    log_app_id=LOG_APP_ID)
            # 导入插件 - END

            # 开始执行request插件,替代框架的下载-START
            plugin_step_name = 'request'
            if import_current_layer_module and hasattr(import_current_layer_module,
                                                       plugin_step_name) and callable(
                getattr(import_current_layer_module, plugin_step_name)):
                LOGGER.other_op.info(
                    f'''配置表sourceid：{sourceid} - {DATA_DOWNLOAD_FLAG} - {task_type} - 网址:{url} - 插件处理：{plugin_step_name} - Start''')
                _plugin_result = ''
                try:
                    headers_plugin_temp_dict = dict(
                        [line.replace(': ', ':').split(":", 1) for line in headers.split("\n")])
                    if isinstance(url, str):
                        _plugin_result = import_current_layer_module.request(url,
                                                                             headers_plugin_temp_dict,
                                                                             post_data, charset,
                                                                             proxies,
                                                                             record['isjson'])
                    elif isinstance(url, tuple) or isinstance(url, list):
                        _plugin_result = import_current_layer_module.request(
                            url[0],
                            headers_plugin_temp_dict,
                            url[1],
                            charset,
                            proxies,
                            record['isjson'])
                    else:
                        LOGGER.req.warning(
                            f'配置表sourceid：{sourceid} - {DATA_DOWNLOAD_FLAG} - {task_type} - 网址:{url} - 不支持的网址格式')
                        error_flag = 1
                    resource = _plugin_result
                except Exception as e:
                    LOGGER.other_op.debug(
                        f'''配置表sourceid：{sourceid} - {DATA_DOWNLOAD_FLAG} - {task_type} - 网址:{url} - 插件处理报错：{plugin_step_name} - {str(traceback.format_exc())}''')
                    raise Exception(
                        f'''配置表sourceid：{sourceid} - {DATA_DOWNLOAD_FLAG} - {task_type} - 网址:{url} - 插件处理报错：{plugin_step_name} - {str(traceback.format_exc())}''')
                LOGGER.other_op.info(
                    f'''配置表sourceid：{sourceid} - {DATA_DOWNLOAD_FLAG} - {task_type} - 网址:{url} - 插件处理：{plugin_step_name} - End''')
            # 插件处理-END

            else:
                # 开始执行before_init_url插件-START
                plugin_before_step_name = 'before_request'
                if import_current_layer_module and hasattr(import_current_layer_module,
                                                           plugin_before_step_name) and callable(
                    getattr(import_current_layer_module, plugin_before_step_name)):
                    LOGGER.other_op.info(
                        f'''配置表sourceid：{sourceid} - {DATA_DOWNLOAD_FLAG} - {task_type} - 网址:{url} - 插件处理：{plugin_before_step_name} - Start''')
                    try:
                        headers_plugin_temp_dict = dict(
                            [line.replace(': ', ':').split(":", 1) for line in headers.split("\n")])
                        if isinstance(url, str):
                            _plugin_result = import_current_layer_module.before_request(url,
                                                                                        headers_plugin_temp_dict,
                                                                                        post_data,
                                                                                        charset,
                                                                                        proxies,
                                                                                        record[
                                                                                            'isjson'])
                            url = _plugin_result['url'] if _plugin_result.get('url', None) else url
                            headers = _plugin_result['headers'] if _plugin_result.get('headers',
                                                                                      None) else headers
                            post_data = _plugin_result['post_data'] if _plugin_result.get(
                                'post_data', None) else post_data
                            charset = _plugin_result['charset'] if _plugin_result.get('charset',
                                                                                      None) else charset
                        elif isinstance(url, tuple) or isinstance(url, list):
                            _plugin_result = import_current_layer_module.before_request(
                                url[0],
                                headers_plugin_temp_dict,
                                url[1],
                                charset,
                                proxies,
                                record['isjson'])
                            url_t = _plugin_result['url'] if _plugin_result.get('url', None) else \
                                url[0]
                            post_data_t = _plugin_result['post_data'] if _plugin_result.get(
                                'post_data', None) else url[1]
                            url = (url_t, post_data_t)
                            headers = _plugin_result['headers'] if _plugin_result.get('headers',
                                                                                      None) else headers
                            charset = _plugin_result['charset'] if _plugin_result.get('charset',
                                                                                      None) else charset
                            proxies = _plugin_result['proxies'] if _plugin_result.get('proxies',
                                                                                      None) else proxies
                    except Exception as e:
                        LOGGER.other_op.debug(
                            f'''配置表sourceid：{sourceid} - {DATA_DOWNLOAD_FLAG} - {task_type} - 网址:{url} - 插件处理报错：{plugin_before_step_name} - {str(traceback.format_exc())}''')
                        raise Exception(
                            f'''配置表sourceid：{sourceid} - {DATA_DOWNLOAD_FLAG} - {task_type} - 网址:{url} - 插件处理报错：{plugin_before_step_name} - {str(traceback.format_exc())}''')
                    LOGGER.other_op.info(
                        f'''配置表sourceid：{sourceid} - {DATA_DOWNLOAD_FLAG} - {task_type} - 网址:{url} - 插件处理：{plugin_before_step_name} - End''')
                # 插件处理-END

                if isinstance(url, str):
                    # get
                    LOGGER.other_op.info(
                        f'配置表sourceid：{sourceid} - {DATA_DOWNLOAD_FLAG} - {task_type} - 网址请求 - 网址:{url} - GET - Start')
                    try:
                        resource = self.REQ.get_html(url=url, headers=headers, postdata=post_data,
                                                     charset=charset, proxies=proxies,
                                                     env=db_env)
                    except Exception as e:
                        # LOGGER.req.debug(
                        #     f'配置表sourceid：{sourceid} - {DATA_DOWNLOAD_FLAG} - 网址:{url} - GET - 请求失败 - {str(traceback.format_exc())}')
                        error_flag = 1
                elif isinstance(url, tuple) or isinstance(url, list):
                    # post
                    LOGGER.other_op.info(
                        f'配置表sourceid：{sourceid} - {DATA_DOWNLOAD_FLAG} - {task_type} - 网址:{url} - 网址请求 - POST - Start')
                    try:
                        resource = self.REQ.get_html(url=url[0], postdata=url[1], headers=headers,
                                                     charset=charset, proxies=proxies, env=db_env)
                    except Exception as e:
                        # LOGGER.req.debug(
                        #     f'配置表sourceid：{sourceid} - {DATA_DOWNLOAD_FLAG} - 网址:{url} - POST - 请求失败 - {str(traceback.format_exc())}')
                        error_flag = 1
                else:
                    LOGGER.req.warning(
                        f'配置表sourceid：{sourceid} - {DATA_DOWNLOAD_FLAG} - {task_type} - 网址:{url} - 网址请求失败：不支持的网址格式')
                    resource = {'res_code': 'ERROR'}

            if error_flag == 1:
                if task_type == 'test':
                    if not resource:
                        # 防止日志打印报错
                        resource = {"res_code": ""}
                    LOGGER.req.warning(
                        f'配置表sourceid：{sourceid} - {DATA_DOWNLOAD_FLAG} - {task_type} - 网址:{url} - 网址请求失败：{resource.get("res_code", "")}!')
                    # listResults = [{"_url": resource['res_code']}]
                    return None
                else:
                    raise Exception(
                        f'配置表sourceid：{sourceid} - {DATA_DOWNLOAD_FLAG} - {task_type} - 网址:{url} - 网址请求失败!')

            if resource and resource['res_code'] == '200' and resource['resource']:
                LOGGER.other_op.info(
                    f'配置表sourceid：{sourceid} - {DATA_DOWNLOAD_FLAG} - {task_type} - 网址:{url} - 网址请求成功 - End')
                # 开始执行after_request插件,框架下载网页成功后的处理 -START
                plugin_after_step_name = 'after_request'
                if import_current_layer_module and hasattr(import_current_layer_module,
                                                           plugin_after_step_name) and callable(
                    getattr(import_current_layer_module, plugin_after_step_name)):
                    LOGGER.other_op.info(
                        f'''配置表sourceid：{sourceid} - {DATA_DOWNLOAD_FLAG} - {task_type} - 网址:{url} - 插件处理：{plugin_after_step_name} - Start''')
                    _plugin_result = ''
                    try:
                        _plugin_result = import_current_layer_module.after_request(
                            resource['resource'])
                        resource['resource'] = _plugin_result
                    except Exception as e:
                        LOGGER.other_op.debug(
                            f'''配置表sourceid：{sourceid} - {DATA_DOWNLOAD_FLAG} - {task_type} - 网址:{url} - 插件处理报错：{plugin_after_step_name} - {str(traceback.format_exc())}''')
                        raise Exception(
                            f'''配置表sourceid：{sourceid} - {DATA_DOWNLOAD_FLAG} - {task_type} - 网址:{url} - 插件处理报错：{plugin_after_step_name} - {str(traceback.format_exc())}''')
                    LOGGER.other_op.info(
                        f'''配置表sourceid：{sourceid} - {DATA_DOWNLOAD_FLAG} - {task_type} - 网址:{url} - 插件处理：{plugin_after_step_name} - End''')
                # 插件处理-END

                if isinstance(url, tuple) or isinstance(url, list):
                    url = url[0] + '#post#=' + url[1]

                new_dict = {
                    "_url": url,
                    "sourceid": str(sourceid),
                    "grabdatacode": str(record['grabdatacode']),
                    "current_layer": int(current_layer),
                    "total_layers": int(total_layers),
                    "task_type": task_type,
                    "program_flag": DATA_DOWNLOAD_FLAG,
                    "all_layer_data": record.get('all_layer_data', []),
                    "resource": resource,
                    'msg_priority': record.get('msg_priority', 0),
                }
                # self.add_config(new_dict, record)
                if task_type == 'test':
                    return new_dict
                elif db_env == '185':
                    queue_name = TEST_LC_JZ_DATA_TO_BE_EXTRACT_QUEUE_NAME
                elif db_env == 'formal':
                    queue_name = LC_JZ_DATA_TO_BE_EXTRACT_QUEUE_NAME
                else:
                    raise Exception(
                        f'配置表sourceid：{sourceid} - {DATA_DOWNLOAD_FLAG} - {task_type} - 未知的运行环境：{db_env}')

                try:
                    new_dict['send_to_next_mq'] = queue_name
                    new_dict['msg_create_time'] = datetime.datetime.now().strftime(
                        '%Y-%m-%d %H:%M:%S')
                    new_dict['modulecode'] = record.get('modulecode', '')
                    second_msg = json.dumps(new_dict, ensure_ascii=False)
                    # send = OhMyMqSender(queue_name=queue_name,
                    #                     priority=15,
                    #                     mq_env=mq_env)  # 历史数据消息等级2 ，日常消息等级15
                    global MQ_Manager
                    if not MQ_Manager:
                        MQ_Manager = RabbitMqManager(mq_env)
                    MQ_Manager.queue_declare(queue_name=queue_name,
                                             x_max_priority=10,
                                             prefetch_count=1,
                                             bind_dead_queue=True)
                    MQ_Manager.send_json_msg(queue_name=queue_name, json_str=second_msg,
                                             priority=record.get('msg_priority', 0))
                    LOGGER.other_op.info(
                        f'''配置表sourceid：{sourceid} - {DATA_DOWNLOAD_FLAG} - {task_type} - 网址:{url} - 消息推送至{queue_name}成功！''')
                except Exception as e:
                    LOGGER.other_op.debug(
                        f'''配置表sourceid：{sourceid} - {DATA_DOWNLOAD_FLAG} - {task_type} - 网址:{url} - 消息推送至{queue_name}失败！- {str(traceback.format_exc())}''')
                    raise Exception(
                        f'''配置表sourceid：{sourceid} - {DATA_DOWNLOAD_FLAG} - {task_type} - 网址:{url} - {new_dict} - 消息推送至{queue_name}失败！- {str(traceback.format_exc())}''')
                else:
                    if db_env == '185':
                        # 185留痕是为了测试
                        # 只有正式环境才留痕，消息流转到下一环节再留痕，留痕失败也不影响正常数据抓取
                        try:
                            self.save_source_code_to_file(resource['resource'],
                                                          record.get('modulecode', ''),
                                                          sourceid,
                                                          current_layer,
                                                          url)
                        except Exception as e:
                            LOGGER.other_op.warning(
                                f'配置表sourceid：{sourceid} - {DATA_DOWNLOAD_FLAG} - {task_type} - 网址:{url} - 留痕异常 - {str(traceback.format_exc())}')
                        else:
                            LOGGER.other_op.info(
                                f'配置表sourceid：{sourceid} - {DATA_DOWNLOAD_FLAG} - {task_type} - 网址:{url} - 留痕成功')
            else:
                LOGGER.req.warning(
                    f'配置表sourceid：{sourceid} - {DATA_DOWNLOAD_FLAG} - {task_type} - 网址:{url}- 网址请求失败：{resource.get("resource", "")}!')
                if task_type == 'test':
                    return None
                else:
                    if not resource:
                        # 防止日志打印报错
                        resource = {"res_code": "", "resource": ''}
                    raise Exception(
                        f'配置表sourceid：{sourceid} - {DATA_DOWNLOAD_FLAG} - {task_type} - 网址:{url}- 网址请求失败：获取的网址有效数据为空,状态码为：{resource.get("res_code", "")}-返回体为：{resource["resource"]}')
            try:
                if import_current_layer_module:
                    del sys.modules[import_current_layer_module]
            except Exception as e:
                LOGGER.warning(
                    f'sourceid:{sourceid} - {DATA_DOWNLOAD_FLAG} - {task_type} - 删除导入的插件模块失败，不影响抓取数据操作：{str(traceback.format_exc())}')

    @staticmethod
    def get_all_field_name_and_value(all_field_data_list):
        # 获取所有已抽取的字段名称，不允许有重复的
        all_field_name = []
        # 所有已抓取的字段key-value合集
        all_field_obj = {}
        if isinstance(all_field_data_list, list):
            all_field = [x['fields'] for x in all_field_data_list]
            for y in all_field:
                if 'record_html' in y.keys():
                    y.pop('record_html')
                if '_url' in y.keys():
                    y.pop('_url')
                if '_content' in y.keys():
                    y.pop('_content')
                if 'current_layer' in y.keys():
                    y.pop('current_layer')
                if 'total_layers' in y.keys():
                    y.pop('total_layers')
                all_field_name.extend(y.keys())
                all_field_obj.update(y)
        elif isinstance(all_field_data_list, dict):
            if 'record_html' in all_field_data_list.keys():
                all_field_data_list.pop('record_html')
            if '_url' in all_field_data_list.keys():
                all_field_data_list.pop('_url')
            if '_content' in all_field_data_list.keys():
                all_field_data_list.pop('_content')
            if 'current_layer' in all_field_data_list.keys():
                all_field_data_list.pop('current_layer')
            if 'total_layers' in all_field_data_list.keys():
                all_field_data_list.pop('total_layers')
            all_field_name = all_field_data_list.keys()
            all_field_obj = all_field_data_list
        else:
            pass

        return all_field_obj, all_field_name

    @staticmethod
    def save_source_code_to_file(param, module_code, sourceid, layer, url):
        if not module_code:
            raise Exception(f'业务模块编码为空，无法获取对应的留痕目录')
        # file_path = self.get_file_path_from_module(module_code, sourceid, layer, url)
        file_path = get_file_path_from_module(SAVE_SOURCE_CODE_FILE_PATH, module_code, sourceid,
                                              layer, url)
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(url + "</br>")
            f.write(param)


def get_db_config(item):
    # LOGGER = KafkaLogger(name=LOGGER_NAME, appid=LOG_APP_ID)
    sourceid = item['sourceid']
    current_layer = item['current_layer']
    total_layers = item['total_layers']
    url = item["_url"]
    if current_layer == 1 or current_layer == total_layers:
        get_config_sql = f'''SELECT TOP 1 * FROM  RReportTask..ct_finprod_nav_config where 
        sourceid='{sourceid}' '''
    else:
        get_config_sql = f'''SELECT TOP 1 a.*,b.grabdatacode,b.useproxy,b.proxys,b.modulecode,b.rawurl,b.msg_priority FROM  
        RReportTask..ct_finprod_nav_config_layer a inner join  
        RReportTask..ct_finprod_nav_config b on a.sourceid=b.sourceid where a.sourceid='{sourceid}' and a.layer={current_layer} '''
    records = OPSQL.query(get_config_sql)
    record = None
    if records:
        record = records[0]
        for k, v in record.items():
            try:
                if v is None:
                    record[k] = ''
                if type(v) == str:
                    if k == 'header':
                        pattern = re.compile("[\u4e00-\u9fa5]")  # Unicode范围内的汉字编码
                        s = v.encode('latin1').decode('gbk')
                        if re.search(pattern, s) is not None:
                            record[k] = s
            except Exception as e:
                LOGGER.other_op.error(str(traceback.format_exc()))
        record['_url'] = url.replace('&amp;', '&')
        record['total_layers'] = total_layers
        record['current_layer'] = current_layer
        record['task_type'] = item['task_type']
        record["program_flag"] = item.get('program_flag', '')
        record['all_layer_data'] = item.get('all_layer_data', [])
        if item.get('task_type', ''):
            item.pop('task_type')
        if item.get('program_flag', ''):
            item.pop('program_flag')
        if item.get('all_layer_data', ''):
            item.pop('all_layer_data')
        if item.get('current_layer', ''):
            item.pop('current_layer')
        if item.get('total_layers', ''):
            item.pop('total_layers')
        if item.get('sourceid', ''):
            item.pop('sourceid')
        record.update(item)

    return record


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
        record = get_db_config(body_dict)
        downloader = DataDownloader(REQ)
        downloader.run(record)
    except Exception as e:
        LOGGER.other_op.warning(f'消息消费失败：{str(traceback.format_exc())},消息体：{body_temp}')
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    else:
        ch.basic_ack(delivery_tag=method.delivery_tag)


def data_download(test_param=None):
    # global REQ, OPSQL, MONGO
    # downloader = DataDownloader(REQ, OPSQL, OhMyExtractor, Parser, MONGO)
    downloader = DataDownloader(REQ)

    # 配置表提取语句
    if test_param and test_param['task_type'] == 'test':
        # listener.start()  # 启动发送线程_界面测试
        # if test_param['total_layers'] <= SPIDER_LAYER_2:
        #     # 当前程序只处理多层级-层级>2
        #     return []
        record = get_db_config(test_param)
        re_info = downloader.run(record)
        return re_info
    else:
        listener.start()  # 启动发送线程
        macip = OhMyExtractor.get_ip()
        assert macip
        LOGGER.other_op.debug(f'00 - 当前macip:{macip}')
        LOGGER.other_op.info("00 - 下载网页程序启动!")
        #
        # pool = ThreadPoolExecutor(max_workers=10)
        # record_queue = queue.Queue(maxsize=10)

        if db_env == '185':
            queue_name = TEST_LC_JZ_DATA_TO_BE_DOWNLOADED_QUEUE_NAME
        elif db_env == 'formal':
            queue_name = LC_JZ_DATA_TO_BE_DOWNLOADED_QUEUE_NAME
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


if __name__ == "__main__":
    data_download(test_param=None)
    # go_list(test_param={"sourceid": "69066503-C605-3F5F-A62D-05B69B60C6C6",
    #                     "_url": "https://www.cindasc.com/servlet/json", "task_type": "test"})
