#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Project ：  spider-nav-config-platform 
@File    :   data_extract_threads.py
@Time    :   2024/3/15 9:43
@Author  :   MengJia_Fang 501738
@E-mail  :   fangmj@finchina.com
@License :   (C)Copyright Financial China Information & Technology Co., Ltd.
@Desc    :   Description
@Version :   1.0
"""
import datetime
import html
import json
import os
import random
import re
import sys
import traceback

parent_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(parent_path)

from spider_logging.kafka_logging import start_hear_beat, listener
from spider_logging.kafkalogger import KafkaLogger
import utils.env
import utils.settings
from utils.parser import ParserJson, Parser
from utils.dbUtil import OhMyDb
from utils.jz_utils import extract_gen_url_list, handle_date_fmt, import_module_from_str, \
    create_data_guid
from utils.mqUtil import RabbitMqManager
from utils.spider_constant import LC_JZ_DATA_TO_BE_DEDUPLICATED_QUEUE_NAME, \
    TEST_LC_JZ_DATA_TO_BE_DEDUPLICATED_QUEUE_NAME, TEST_LC_JZ_DATA_TO_BE_STORED_QUEUE_NAME, \
    LC_JZ_DATA_TO_BE_STORED_QUEUE_NAME, LC_JZ_DATA_TO_BE_DOWNLOADED_QUEUE_NAME, \
    TEST_LC_JZ_DATA_TO_BE_DOWNLOADED_QUEUE_NAME, DATA_EXTRACT_FLAG, \
    TEST_LC_JZ_DATA_TO_BE_EXTRACT_QUEUE_NAME, LC_JZ_DATA_TO_BE_EXTRACT_QUEUE_NAME, \
    TEST_LC_JZ_File_TO_BE_EXTRACT_QUEUE_NAME, LC_JZ_File_TO_BE_EXTRACT_QUEUE_NAME
from utils.tools import OhMyExtractor

LOG_APP_ID = '042bb6121d41462b8ebcf3f8029911d2'
LOGGER_NAME = 'spider.data_extract' + "_" + str(random.randint(1, 1000))
LOGGER = KafkaLogger(name=LOGGER_NAME, appid=LOG_APP_ID)
start_hear_beat(print_console=True, app_id=LOG_APP_ID)

# 数据库环境 test/formal
db_env = utils.env.DB_ENV
# 消息队列环境 test/formal
mq_env = utils.env.MQ_ENV

# mongodb环境 test/formal
mongo_env = utils.env.MONGO_ENV

OPSQL = OhMyDb(db_env)

# MONGO = pymongo.MongoClient(utils.settings.MONGO_INFO[mongo_env]['CLIENT'])[
#     utils.settings.MONGO_INFO[mongo_env]['DB']][
#     utils.settings.MONGO_INFO[mongo_env]['COL_MID_DATA']]
# path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
# sys.path.append(path)

plugin_path = os.path.join(
    os.path.abspath(os.path.dirname(os.path.abspath(__file__)) + os.path.sep), 'plugins')
sys.path.append(plugin_path)
MQ_Manager = ''


class DataExtractor:
    def __init__(self, extractor, parser):
        self.extractor = extractor
        self.Parser = parser
        self.ParserJson = ParserJson

    def run(self, record=None):
        global db_env
        global MQ_Manager
        # thread_logger = KafkaLogger(name=LOGGER_NAME, appid=LOG_APP_ID)
        if not record.get('grabdatacode', '') or record['grabdatacode'] == 'None':
            LOGGER.set_data_source_id('0', durable=True)
        else:
            LOGGER.set_data_source_id(record['grabdatacode'], durable=True)

        originTime1 = datetime.datetime.now().strftime("%Y-%m-%d")
        originTime2 = datetime.datetime.now().strftime("%H:%M:%S")

        LOGGER.set_origin_msg_time(f'{originTime1} {originTime2}.000')

        resource = record.get('resource', '')
        # 源码预处理
        res = resource['resource']
        res = re.sub(r'\s', r' ', res)
        res = re.sub(r'\\u200[BCDEFbcdef]', r' ', res)
        res = res.replace('\u200b', '')
        # list_html = html.unescape(list_html)

        sourceid = record['sourceid']
        current_layer = record['current_layer']
        total_layers = record['total_layers']
        task_type = record.get('task_type', '')
        url = record['_url']

        if current_layer == 1 or total_layers == 1:
            # 第一级网址的抓取配置从主配置表字段取
            if record['isjson']:
                record['isjson'] = record['isjson'].split(',')[0]
            record['field_rule'] = record['layer_0_field_rule']
            record['plugin'] = record['layer_0_plugin']
        elif current_layer == total_layers:
            # 第一级网址的抓取配置从主配置表字段取
            if record['isjson']:
                record['isjson'] = record['isjson'].split(',')[1]
            record['field_rule'] = record['conrule']
            record['plugin'] = record['content_plugin']
        else:
            record['field_rule'] = record['field_rule']
            record['isjson'] = re.sub('"', "", str(record['is_json']))
            record['plugin'] = record['plugin']

        # 从消息体中获取,all_mid_layer_data里面的第一层是有生成的初始网址的
        all_mid_layer_data = record.get('all_layer_data', '')
        all_field_obj, all_field_name = self.get_all_field_name_and_value(all_mid_layer_data)
        error_flag = False
        listResults = []
        # 导入插件模块 - START
        import_current_layer_module = None
        import_module_name = f'{sourceid}_{current_layer}_{DATA_EXTRACT_FLAG}'
        if record.get('plugin', ''):
            import_current_layer_module = import_module_from_str(
                import_module_name,
                record['plugin'],
                log_name=LOGGER_NAME,
                log_app_id=LOG_APP_ID)
        # 导入插件 - END

        # 开始执行extract插件,替代框架当前层的抽取-START
        plugin_step_name = 'extract'
        if import_current_layer_module and hasattr(import_current_layer_module,
                                                   plugin_step_name) and callable(
            getattr(import_current_layer_module, plugin_step_name)):
            LOGGER.other_op.info(
                f'''配置表sourceid：{sourceid} - {DATA_EXTRACT_FLAG} - {task_type} - 网址:{url} - 插件处理：{plugin_step_name} - Start''')
            try:
                _plugin_result = import_current_layer_module.extract(res, record['field_rule'],
                                                                     all_field_obj, all_field_name,
                                                                     url)

                listResults = _plugin_result
            except Exception as e:
                # LOGGER.other_op.warning(
                #     f'''配置表sourceid：{sourceid} - {DATA_EXTRACT_FLAG} - {task_type} - 网址:{url} - 插件处理报错：{plugin_step_name} - 抽取规则：{record.get("field_rule", "")} - exception：{str(traceback.format_exc())}''')
                raise Exception(
                    f'''配置表sourceid：{sourceid} - {DATA_EXTRACT_FLAG} - {task_type} - 网址:{url} - 插件处理报错：{plugin_step_name} - 抽取规则：{record.get("field_rule", "")} - exception：{str(traceback.format_exc())}''')
            LOGGER.other_op.info(
                f'''配置表sourceid：{sourceid} - {DATA_EXTRACT_FLAG} - {task_type} - 网址:{url} - 插件处理：{plugin_step_name} - End''')
            # 插件处理-END
        else:
            # TODO: 开始抽取
            try:
                LOGGER.other_op.info(
                    f'配置表sourceid：{sourceid} - {DATA_EXTRACT_FLAG} - {task_type} - 网址:{url} - 数据抽取 - Start')
                if int(record['isjson']) == 0:
                    try:
                        parser = self.Parser(res, record['field_rule'])
                    except Exception as e:
                        raise Exception(
                            f'配置表sourceid：{sourceid} - {DATA_EXTRACT_FLAG} - {task_type} - 网址:{url} - 数据抽取报错 - url对应的返回体转换为Parser解析对象失败，请检查url对应的返回体是不是html类型：{str(traceback.format_exc())}')
                    else:
                        listResults = parser.extraction_html(all_field_obj, all_field_name, url)
                elif int(record['isjson']) == 1:
                    try:
                        parser = self.ParserJson(res, record['field_rule'])
                    except Exception as e:
                        raise Exception(
                            f'配置表sourceid：{sourceid} - {DATA_EXTRACT_FLAG} - {task_type} - 网址:{url} - 数据抽取报错 - url对应的返回体转换为arser_Json解析对象失败，请检查url对应的返回体是不是json类型：{str(traceback.format_exc())}')
                    else:
                        listResults = parser.extraction_json(all_field_obj, all_field_name, url)
            except Exception as e:
                LOGGER.extract.debug(
                    f'配置表sourceid：{sourceid} - {DATA_EXTRACT_FLAG} - {task_type} - 网址:{url} - 数据抽取报错 - 抽取规则：{record.get("field_rule", "")} - exception：{str(traceback.format_exc())}')

                if task_type == 'test':
                    listResults = [
                        {
                            "_url": f'配置表sourceid：{sourceid} - {DATA_EXTRACT_FLAG} - {task_type} - 网址:{url} - 数据抽取报错 - 抽取规则：{record.get("field_rule", "")} - exception：{str(traceback.format_exc())}'}
                    ]
                    if current_layer == total_layers:
                        listResults = [{
                            "_url": '抽取报错了',
                            "fields": [{
                                "暂无数据": f'配置表sourceid：{sourceid} - {DATA_EXTRACT_FLAG} - {task_type} - 网址:{url} - 数据抽取报错 - 抽取字段：{record.get("field_rule", "")} - exception：{str(traceback.format_exc())}'}],
                            "_content_final": '',
                        }]
                    return listResults
                else:
                    error_flag = True
        if not error_flag:
            # 补全链接
            # if listResults and 'url' in listResults[0].keys():
            for listResult in listResults:
                for key in listResult.keys():
                    if 'url' in key and listResult[key]:
                        listResult[key] = html.unescape(
                            self.extractor.complete_url(record['rawurl'],
                                                        listResult[key]))
                        listResult[key] = listResult[key].replace(r"'", "''")
            if current_layer == total_layers:
                # 最后一层，没有下一层了
                if task_type == 'test':
                    # 如果是测试则不去重,直接返回抓取结果
                    if len(listResults) == 0:
                        listResults = [{"_url": '未抽取到符合规则的数据信息'}]
                    # 判断是否需要进行附件自动抽取
                    content_is_auto_file = record.get('content_is_auto_file', None)
                    for l_r in listResults:
                        # if l_r.get('record_html', ''):
                        #     l_r.pop('record_html')
                        l_r.update(all_field_obj)
                        # 最后一层不需要+1了
                        l_r['current_layer'] = total_layers
                        l_r['total_layers'] = total_layers

                    # a 是最后一页特殊的返回方式
                    # content_is_auto_file、modulecode、grabdatacode、msg_priority都是为了测试数据的附件抽取，返回前端前已经去掉了
                    a = [{
                        "_url": url,
                        "fields": listResults,
                        "_content_final": resource['resource'],
                        'content_is_auto_file': content_is_auto_file if content_is_auto_file else '',
                        'modulecode': record.get('modulecode', ''),
                        'grabdatacode': str(record['grabdatacode']),
                        'msg_priority': record.get('msg_priority', 0),
                    }]
                    # return listResults
                    LOGGER.other_op.info(
                        f'配置表sourceid：{sourceid} - {DATA_EXTRACT_FLAG} - {task_type} - 网址:{url} - 数据抽取 - end')
                    return a
                else:
                    # 当前是最后一层数据，解析后，如果需要自动附件抽取就去附件抽取队列，不需要自动附件抽取，就丢到去重队列，是否需要去重都在去重程序处理
                    for result in listResults:
                        c_url = result.get('_url', '')
                        if c_url:
                            result.pop('_url')
                        if result.get('record_html', ''):
                            result.pop('record_html')
                        if result.get('_content', ''):
                            result.pop('_content')
                        # 将所有字段合并
                        result.update(all_field_obj)
                        self.add_config(result, record)

                        data_guid = create_data_guid(sourceid, record.get('repeatrule', ''), result,
                                                     LOGGER_NAME,
                                                     LOG_APP_ID, DATA_EXTRACT_FLAG)
                        if not data_guid:
                            LOGGER.de_duplication.error(
                                f'配置表sourceid:{sourceid} - {task_type} - 网址：{record.get("_url", "")} - 数据抽取报错 - 生成的数据guid为空：{result}')
                            raise Exception(
                                f'配置表sourceid:{sourceid} - {task_type} - 网址：{record.get("_url", "")} - 数据抽取报错 - 生成的数据guid为空：{result}')

                        # 去重范围
                        dup_range = record.get('repeatrange', '')
                        multi_guid_rule = record.get('multiguidrule', '')
                        new_dict = {
                            "_url": c_url,
                            "sourceid": str(sourceid),
                            "grabdatacode": str(record['grabdatacode']),
                            'modulecode': record.get('modulecode', ''),
                            'content_is_auto_file': record.get('content_is_auto_file', None),
                            "dup_rule": record.get('repeatrule', ''),
                            "multi_guid_rule": multi_guid_rule,
                            "dup_range": dup_range,
                            "stored_fields": result,
                            "data_guid": data_guid,
                            "program_flag": DATA_EXTRACT_FLAG,
                            "task_type": task_type,
                            'msg_priority': record.get('msg_priority', 0),
                        }

                        # 判断是否需要进行附件自动抽取
                        content_is_auto_file = record.get('content_is_auto_file', None)
                        if content_is_auto_file:
                            # 需要丢入自动抽取附件的消息队列
                            if db_env == '185':
                                queue_name = TEST_LC_JZ_File_TO_BE_EXTRACT_QUEUE_NAME
                            elif db_env == 'formal':
                                queue_name = LC_JZ_File_TO_BE_EXTRACT_QUEUE_NAME
                            else:
                                raise Exception(
                                    f'配置表sourceid:{sourceid} - {DATA_EXTRACT_FLAG} - {task_type} - 下载表guid:{data_guid} - 需要进行附件抽取 - 报错信息：未知的运行环境：{db_env}')
                        else:
                            if multi_guid_rule:
                                # 说明是多表数据，又没有勾选附件抽取，直接生成子表guid
                                sub_data_guid = create_data_guid(sourceid,
                                                                 multi_guid_rule,
                                                                 result,
                                                                 LOGGER_NAME,
                                                                 LOG_APP_ID, DATA_EXTRACT_FLAG)
                                if not sub_data_guid:
                                    # LOGGER.de_duplication.error(
                                    #     f'配置表sourceid:{sourceid} - 网址：{record.get("_url", "")} - 数据抽取失败 - 生成的子表数据sub_data_guid为空：{result}')
                                    raise Exception(
                                        f'配置表sourceid:{sourceid} - {DATA_EXTRACT_FLAG} - {task_type} - 网址：{record.get("_url", "")} - 数据抽取报错 - 生成的子表数据sub_data_guid为空：{result}')
                                new_dict['sub_data_guid'] = sub_data_guid
                            # 直接发送到待去重队列
                            if db_env == 'formal':
                                queue_name = LC_JZ_DATA_TO_BE_DEDUPLICATED_QUEUE_NAME
                            elif db_env == '185':
                                queue_name = TEST_LC_JZ_DATA_TO_BE_DEDUPLICATED_QUEUE_NAME
                            else:
                                raise Exception(
                                    f'配置表sourceid:{sourceid} - {DATA_EXTRACT_FLAG} - {task_type} - 下载表guid:{data_guid} - 需要进行附件抽取 - 报错信息：未知的运行环境：{db_env}')
                        try:
                            new_dict['send_to_next_mq'] = queue_name
                            new_dict['msg_create_time'] = datetime.datetime.now().strftime(
                                '%Y-%m-%d %H:%M:%S')
                            second_msg = json.dumps(new_dict, ensure_ascii=False)
                            # send = OhMyMqSender(queue_name=queue_name,
                            #                     priority=15,
                            #                     mq_env=mq_env)  # 历史数据消息等级2 ，日常消息等级15
                            if not MQ_Manager:
                                MQ_Manager = RabbitMqManager(mq_env)
                            MQ_Manager.queue_declare(queue_name=queue_name,
                                                     x_max_priority=10,
                                                     prefetch_count=1,
                                                     bind_dead_queue=True)
                            MQ_Manager.send_json_msg(queue_name=queue_name, json_str=second_msg,
                                                     priority=record.get('msg_priority', 0))
                            LOGGER.other_op.info(
                                f'''配置表sourceid:{sourceid} - {DATA_EXTRACT_FLAG} - {task_type} - 网址:{url} - {result} - 消息推送至{queue_name}成功！''')
                        except Exception as e:
                            # LOGGER.other_op.info(
                            #     f'''配置表sourceid:{sourceid} - {DATA_EXTRACT_FLAG} - {task_type} - 网址:{url} - {result} - 数据抽取报错：消息推送至{queue_name}失败！- {str(traceback.format_exc())}''')
                            raise Exception(
                                f'''配置表sourceid:{sourceid} - {DATA_EXTRACT_FLAG} - {task_type} - 网址:{url} - {result} - 数据抽取报错：消息推送至{queue_name}失败！- {str(traceback.format_exc())}''')
            else:
                if current_layer + 1 == total_layers:
                    # 下一层是第一层和最后一层 从主配置表获取配置(正文的配置)
                    get_next_layer_config_sql = f'''SELECT TOP 1 * FROM  RReportTask..ct_finprod_nav_config where sourceid='{sourceid}' '''
                else:
                    # 从 从配置表获取下一层级的配置
                    get_next_layer_config_sql = f'''SELECT TOP 1 * FROM  RReportTask..ct_finprod_nav_config_layer where sourceid='{sourceid}' and layer={current_layer + 1}'''
                next_layer_config_records = OPSQL.query(get_next_layer_config_sql)
                if next_layer_config_records:
                    next_layer_config_record = next_layer_config_records[0]
                    postdata = next_layer_config_record['postdata']
                    # post_data_list = []
                    if postdata:
                        postdata = json.loads(postdata)
                        if postdata.get("content_post", None):
                            postdata = postdata['content_post']
                        elif postdata.get("list_post", None):
                            postdata = ''
                        else:
                            postdata = json.dumps(postdata, ensure_ascii=False)
                        # url_date_post_data = re.search('{(year|day)((-|\+)(\d+))?}', postdata)
                        # if url_date_post_data:
                        #     postdata = datecal(postdata, url_date_post_data[1],
                        #                        url_date_post_data[3],
                        #                        url_date_post_data[4])
                        #
                        # # post参数是否存在分页：post_data_list不为空就是存在分页
                        # post_data_list = list(
                        #     get_all_pages(postdata, '', '', '', test_param={"task_type": task_type},
                        #                   log_name=LOGGER_NAME,
                        #                   log_app_id=LOG_APP_ID))
                        # if (not post_data_list) and postdata:
                        #     post_data_list = [postdata]

                    if task_type == 'test':
                        # 如果是测试则不去重,直接返回抓取结果
                        if len(listResults) == 0:
                            listResults = [{"_url": '未抽取到符合规则的数据信息'}]
                        else:
                            # # 只要存在postdata，就默认所有的url都是一样的，所以取第一个拼接post
                            # l_r_0_url = listResults[0]['_url']
                            # post_data_list存在时，new_listResults才有意义
                            new_listResults = []
                            for l_r in listResults:
                                l_r.update(all_field_obj)
                                # +1表示的是当前返回的url所属的层级
                                l_r['current_layer'] = current_layer + 1
                                l_r['total_layers'] = total_layers
                                # 目前暂时不支持中间层网址分页中使用最大页数匹配，以为headers、charset主从配置表不统一以及，还要做一些引用字段、cookie替换
                                l_r['_url'] = handle_date_fmt(l_r['_url'])
                                postdata = handle_date_fmt(postdata)
                                gen_page_list = extract_gen_url_list(l_r['_url'],
                                                                     headers=None,
                                                                     postdata=postdata,
                                                                     pagestyle=l_r['_url'],
                                                                     charset=None,
                                                                     test_param={
                                                                         "task_type": task_type},
                                                                     log_name=LOGGER_NAME,
                                                                     log_app_id=LOG_APP_ID)
                                for p_url in gen_page_list:
                                    new_l_r = {}
                                    new_l_r.update(l_r)
                                    # new_l_r['_url'] = l_r['_url'] + '#post#=' + p_d
                                    if not p_url:
                                        continue
                                    if isinstance(p_url, list) or isinstance(p_url, tuple):
                                        new_l_r['_url'] = p_url[0] + '#post#=' + p_url[1]
                                    else:
                                        new_l_r['_url'] = p_url
                                    new_listResults.append(new_l_r)
                            if new_listResults:
                                listResults = new_listResults
                        LOGGER.other_op.info(
                            f'配置表sourceid：{sourceid} - {DATA_EXTRACT_FLAG} - {task_type} - 网址:{url} - 数据抽取 - end')
                        return listResults
                    else:
                        if db_env == 'formal':
                            queue_name = LC_JZ_DATA_TO_BE_DOWNLOADED_QUEUE_NAME
                        else:
                            queue_name = TEST_LC_JZ_DATA_TO_BE_DOWNLOADED_QUEUE_NAME

                        # 处理下一层的网址以及post参数 分页
                        new_listResults = []
                        for __result in listResults:
                            # 目前暂时不支持中间层网址分页中使用最大页数匹配，以为headers、charset主从配置表不统一以及，还要做一些引用字段、cookie替换
                            __result['_url'] = handle_date_fmt(__result['_url'])
                            postdata = handle_date_fmt(postdata)
                            gen_page_list = extract_gen_url_list(__result['_url'],
                                                                 headers=None,
                                                                 postdata=postdata,
                                                                 pagestyle=__result['_url'],
                                                                 charset=None,
                                                                 test_param={
                                                                     "task_type": task_type},
                                                                 log_name=LOGGER_NAME,
                                                                 log_app_id=LOG_APP_ID)
                            for p_url in gen_page_list:
                                new_l_r = {}
                                new_l_r.update(__result)
                                # new_l_r['_url'] = l_r['_url'] + '#post#=' + p_d
                                if not p_url:
                                    continue
                                if isinstance(p_url, list) or isinstance(p_url, tuple):
                                    new_l_r['_url'] = p_url[0] + '#post#=' + p_url[1]
                                else:
                                    new_l_r['_url'] = p_url
                                new_listResults.append(new_l_r)
                        if new_listResults:
                            listResults = new_listResults

                        for result in listResults:
                            # 来到这一步的all_mid_layer_data肯定是list
                            cur_all_mid_layer_data_list = []
                            cur_all_mid_layer_data_list.extend(all_mid_layer_data)
                            # current_layer_data是用于流转的中间层数据对象
                            current_layer_data = {}
                            if result.get('_url', ''):
                                current_layer_data['_url'] = result['_url']
                                result.pop('_url')
                            if result.get('record_html', ''):
                                current_layer_data['record_html'] = result['record_html']
                                result.pop('record_html')
                            if result.get('_content', ''):
                                current_layer_data['_content'] = result['_content']
                                result.pop('_content')
                            # current_layer_data['current_layer'] = current_layer
                            # current_layer_data['total_layers'] = layers
                            # current_layer_data['sourceid'] = sourceid
                            # current_layer_data['grabdatacode'] = record['grabdatacode']
                            c_l_obj = {"current_layer": current_layer,
                                       "fields": result}
                            cur_all_mid_layer_data_list.append(c_l_obj)
                            next_layer_obj = {
                                '_url': current_layer_data['_url'],
                                'sourceid': sourceid,
                                'grabdatacode': record['grabdatacode'],
                                'modulecode': record.get('modulecode', ''),
                                'total_layers': total_layers,
                                'current_layer': current_layer + 1,
                                "task_type": task_type,
                                "program_flag": DATA_EXTRACT_FLAG,
                                "all_layer_data": cur_all_mid_layer_data_list,
                                'msg_priority': record.get('msg_priority', 0),
                            }
                            # 直接发送到待下载队列
                            try:
                                next_layer_obj['send_to_next_mq'] = queue_name
                                next_layer_obj[
                                    'msg_create_time'] = datetime.datetime.now().strftime(
                                    '%Y-%m-%d %H:%M:%S')
                                second_msg = json.dumps(next_layer_obj, ensure_ascii=False)
                                # send = OhMyMqSender(queue_name=queue_name,
                                #                     priority=15,
                                #                     mq_env=mq_env)  # 历史数据消息等级2 ，日常消息等级15
                                if not MQ_Manager:
                                    MQ_Manager = RabbitMqManager(mq_env)
                                MQ_Manager.queue_declare(queue_name=queue_name,
                                                         x_max_priority=10,
                                                         prefetch_count=1,
                                                         bind_dead_queue=True)
                                MQ_Manager.send_json_msg(queue_name=queue_name, json_str=second_msg,
                                                         priority=record.get('msg_priority', 0))
                                LOGGER.other_op.info(
                                    f'''配置表sourceid：{sourceid} - {DATA_EXTRACT_FLAG} - {task_type} - 网址:{url} - {next_layer_obj} - 消息推送至{queue_name}成功！''')
                            except Exception as e:
                                # LOGGER.other_op.info(
                                #     f'''配置表sourceid：{sourceid} - {DATA_EXTRACT_FLAG} - {task_type} - 网址:{url} - {next_layer_obj} -消息推送至{queue_name}失败！- {str(traceback.format_exc())}''')
                                raise Exception(
                                    f'''配置表sourceid：{sourceid} - {DATA_EXTRACT_FLAG} - {task_type} - 网址:{url} - {next_layer_obj} -消息推送至{queue_name}失败！- {str(traceback.format_exc())}''')
        LOGGER.other_op.info(
            f'配置表sourceid：{sourceid} - {DATA_EXTRACT_FLAG} - {task_type} - 网址:{url} - 数据抽取 - End')

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
    def add_config(result, record):
        result['guid'] = record.get('guid', '')
        result['sourceid'] = record.get('sourceid', '')
        result['grabdatacode'] = record.get('grabdatacode', '')
        result['sitename'] = record.get('sitename', '')
        result['sitesort'] = record.get('sitesort', '')
        result['systemno'] = record.get('systemno', '')
        # result['state'] = record.get('state', '')


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
        get_config_sql = f'''SELECT TOP 1 a.*,b.grabdatacode,b.useproxy,b.proxys,b.repeatrule,b.modulecode,b.rawurl,b.msg_priority,b.multiguidrule FROM  
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
                    if k == 'field_rule':
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
        record["program_flag"] = item["program_flag"]
        record["resource"] = item["resource"]
        record['all_layer_data'] = item.get('all_layer_data', [])
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
        extractor = DataExtractor(OhMyExtractor, Parser)
        extractor.run(record)
    except Exception as e:
        LOGGER.other_op.warning(f'消息消费失败：{str(traceback.format_exc())},消息体：{body_temp}')
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    else:
        ch.basic_ack(delivery_tag=method.delivery_tag)


def data_extract(test_param=None):
    extractor = DataExtractor(OhMyExtractor, Parser)

    # 配置表提取语句
    if test_param and test_param['task_type'] == 'test':
        record = get_db_config(test_param)
        re_info = extractor.run(record)
        return re_info
    else:
        listener.start()  # 启动发送线程
        macip = OhMyExtractor.get_ip()
        assert macip
        LOGGER.other_op.debug(f'00 - 当前macip:{macip}')
        LOGGER.other_op.info("00 - 抽取网页程序启动!")
        # pool = ThreadPoolExecutor(max_workers=10)
        # record_queue = queue.Queue(maxsize=10)

        if db_env == '185':
            queue_name = TEST_LC_JZ_DATA_TO_BE_EXTRACT_QUEUE_NAME
        elif db_env == 'formal':
            queue_name = LC_JZ_DATA_TO_BE_EXTRACT_QUEUE_NAME
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
            LOGGER.other_op.warning(f"消息队列连接失败！ - {str(traceback.format_exc())}")


if __name__ == '__main__':
    data_extract()
