#!/usr/bin/env python
# -*- encoding: utf-8 -*-
"""
@Project ：  spider-nav-config-platform
@File    :   first_layer_url_generate_threads.py
@Time    :   2024/03/13 09:40
@Author  :   MengJia_Fang 501738
@E-mail  :   fangmj@finchina.com
@License :   (C)Copyright Financial China Information & Technology Co., Ltd.
@Desc    :   框架抓取数据的生成初始网址步骤
@Version :   1.0
"""

# here put the import lib
import datetime
import json
import os
import re
import sys

parent_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(parent_path)
import traceback
import utils.env
import utils.settings
from spider_logging.kafka_logging import listener, start_hear_beat
from spider_logging.kafkalogger import KafkaLogger
from utils.dbUtil import OhMyDb
from utils.jz_utils import get_cookie_from_file, get_all_pages, handle_date_fmt, \
    import_module_from_str
from utils.mqUtil import RabbitMqManager
from utils.reqUtil import OhMyReq
from utils.spider_constant import LC_JZ_CRON_TASK_QUEUE_NAME, \
    TEST_LC_JZ_DATA_TO_BE_DOWNLOADED_QUEUE_NAME, LC_JZ_DATA_TO_BE_DOWNLOADED_QUEUE_NAME, \
    FIRST_LAYER_URL_GENERATE_FLAG, DIRECT_RUN_FORMAL_SPIDER_FLAG, TEST_LC_JZ_CRON_TASK_QUEUE_NAME
from utils.tools import OhMyExtractor

LOG_APP_ID = '515de9c59f8042deb021767d62ea04bc'
LOGGER_NAME = 'spider.GEN_LAYER_0_URL'
LOGGER = KafkaLogger(name=LOGGER_NAME, appid=LOG_APP_ID)
start_hear_beat(print_console=True, app_id=LOG_APP_ID)

# 数据库环境 test/formal
db_env = utils.env.DB_ENV
# 消息队列环境 test/formal
mq_env = utils.env.MQ_ENV
# 附件环境 test/formal
file_env = utils.env.FILE_ENV
# mongodb环境 test/formal
mongo_env = utils.env.MONGO_ENV

REQ = OhMyReq()
PY_MSSQL = OhMyDb(db_env)
# MONGO = pymongo.MongoClient(utils.settings.MONGO_INFO[mongo_env]['CLIENT'])[
#     utils.settings.MONGO_INFO[mongo_env]['DB']][
#     utils.settings.MONGO_INFO[mongo_env]['COL_URL']]
# path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
# sys.path.append(path)
plugin_path = os.path.join(
    os.path.abspath(os.path.dirname(os.path.abspath(__file__)) + os.path.sep), 'plugins')
sys.path.append(plugin_path)

MQ_Manager = ''


# sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="gb18030")


class FirstLayerUrlGenerator(object):
    @staticmethod
    def gen_url_list(url, headers=None, postdata=None, pagestyle=None, charset=None,
                     test_param=None):
        # list_urls = []
        # 该字段用于post data分页
        post_data_list = []
        # 该字段用于网址url分页
        url_list = []

        # url是第一条网址,由于之前的代码逻辑，第一条网址不能为空
        if url:
            url = handle_date_fmt(url)
            # # 判断网址需要分页-----第一条网址不允许分页
            # url_list1 = get_all_pages_by_regex_config(url, url, test_param, LOGGER_NAME,
            #                                           LOG_APP_ID)
            # url_list.extend(url_list1)

        # 处理分页：post分页和网址分页
        if postdata:
            postdata = handle_date_fmt(postdata)

            # post参数是否存在分页：post_data_list不为空就是存在分页
            post_data_list = list(
                get_all_pages(postdata, url, headers, charset, test_param, LOGGER_NAME, LOG_APP_ID))
        else:
            postdata = ''

        if pagestyle:
            pagestyle = handle_date_fmt(pagestyle)

            # 网址需要分页
            url_list2 = list(
                get_all_pages(pagestyle, url, headers, charset, test_param, LOGGER_NAME,
                              LOG_APP_ID))
            url_list.extend(url_list2)

        if post_data_list and url_list:
            raise Exception(
                f'正文post参数和网址同时存在分页参数，目前不支持，请检查-正文网址和正文post参数是否配置错误')
        elif url_list:
            url_list.insert(0, url)
            list_urls = url_list
        elif post_data_list:
            list_urls = [(url, p) for p in post_data_list]
        elif postdata:
            list_urls = [(url, postdata)]
        else:
            list_urls = [url]

        return list_urls

    def run(self, record, test_param=None):
        global db_env
        db_env = utils.env.DB_ENV
        # thread_logger = KafkaLogger(name=LOGGER_NAME, appid=LOG_APP_ID)
        if not record.get('grabdatacode', ''):
            LOGGER.set_data_source_id('0', durable=True)
        else:
            LOGGER.set_data_source_id(record['grabdatacode'], durable=True)

        originTime1 = datetime.datetime.now().strftime("%Y-%m-%d")
        originTime2 = datetime.datetime.now().strftime("%H:%M:%S")
        LOGGER.set_origin_msg_time(f'{originTime1} {originTime2}.000')

        sourceid = record['sourceid']
        layers = record['layers']
        task_type = test_param.get('task_type', '')

        layer_0_url_rule = {}
        if record['layer_0_url_rule']:
            layer_0_url_rule = json.loads(record['layer_0_url_rule'])

        # 这个参数是用于第一条url与后面多条规则不统一的时候 单独配置第一条
        layer_0_first_url = layer_0_url_rule.get('url', '')

        # 这两个参数是设置多条有规则的url开始的规则
        layer_0_page_style = layer_0_url_rule.get('pagestyle', '')
        # layer_0_page_rule = layer_0_url_rule.get('pagerule', '')
        # if layer_0_page_rule:
        #     layer_0_page_rule = layer_0_page_rule.encode('latin-1').decode('gbk')
        # layer_0_field_rule = record['layer_0_field_rule']

        layer_0_post_data = ''
        # postdata存储的是初始网址和正文的post参数，key "list_post"是初始网址的post参数
        db_post_data = record['postdata']
        if db_post_data:
            db_post_data = json.loads(db_post_data)
            if 'list_post' in db_post_data:
                layer_0_post_data = db_post_data['list_post']

        layer_0_charset = ''
        db_charset = record['textencoding']
        if db_charset:
            layer_0_charset = (db_charset.split(','))[0]

        layer_0_headers = ''
        if record['header']:
            db_header = json.loads(record['header'])
            if db_header['list_header']:
                layer_0_headers = db_header['list_header']
                cookie_paths = re.findall('#cookie_path=(.+?)#', layer_0_headers.replace('\n', ','),
                                          re.I)
                if cookie_paths:
                    cookie = get_cookie_from_file(cookie_paths[0], LOGGER_NAME, LOG_APP_ID)
                    layer_0_headers = re.sub('#cookie_path=.*#\n', cookie + '\n', layer_0_headers,
                                             re.I)
        # 导入插件 - START
        import_layer_1_module = None
        current_import_module_name = sourceid + f'_1_{FIRST_LAYER_URL_GENERATE_FLAG}'
        if record.get('layer_0_plugin', ''):
            import_layer_1_module = import_module_from_str(current_import_module_name,
                                                           record['layer_0_plugin'],
                                                           log_name=LOGGER_NAME,
                                                           log_app_id=LOG_APP_ID)
        # 导入插件 - END

        # 开始执行before_init_url插件 - START
        plugin_before_step_name = 'before_init_url'
        if import_layer_1_module and hasattr(import_layer_1_module,
                                             plugin_before_step_name) and callable(
            getattr(import_layer_1_module, plugin_before_step_name)):
            LOGGER.other_op.info(
                f'''配置表sourceid：{sourceid} - {FIRST_LAYER_URL_GENERATE_FLAG} - 插件处理：{plugin_before_step_name} - Start''')
            try:
                _plugin_result = import_layer_1_module.before_init_url(
                    layer_0_headers=layer_0_headers, layer_0_post_data=layer_0_post_data,
                    layer_0_first_url=layer_0_first_url, layer_0_page_style=layer_0_page_style)
                layer_0_headers = _plugin_result['layer_0_headers'] if _plugin_result.get(
                    'layer_0_headers', None) else layer_0_headers
                layer_0_post_data = _plugin_result['layer_0_post_data'] if _plugin_result.get(
                    'layer_0_post_data', None) else layer_0_post_data
                layer_0_first_url = _plugin_result['layer_0_first_url'] if _plugin_result.get(
                    'layer_0_first_url', None) else layer_0_first_url
                layer_0_page_style = _plugin_result['layer_0_page_style'] if _plugin_result.get(
                    'layer_0_page_style', None) else layer_0_page_style
            except Exception as e:
                # LOGGER.other_op.warning(
                #     f'''配置表sourceid：{sourceid} - {FIRST_LAYER_URL_GENERATE_FLAG} - {task_type} - 插件处理报错：{plugin_before_step_name} - {str(traceback.format_exc())}''')
                raise Exception(
                    f'''配置表sourceid：{sourceid} - {FIRST_LAYER_URL_GENERATE_FLAG} - {task_type} - 插件处理报错：{plugin_before_step_name} - {str(traceback.format_exc())}''')
            LOGGER.other_op.info(
                f'''配置表sourceid：{sourceid} - {FIRST_LAYER_URL_GENERATE_FLAG} - {task_type} - 插件处理：{plugin_before_step_name} - End''')
        # 插件处理 - END

        # 开始执行init_url插件 - START
        plugin_step_name = 'init_url'
        if import_layer_1_module and hasattr(import_layer_1_module, plugin_step_name) and callable(
                getattr(import_layer_1_module, plugin_step_name)):
            LOGGER.other_op.info(
                f'''配置表sourceid：{sourceid} - {FIRST_LAYER_URL_GENERATE_FLAG} - {task_type} - 插件处理：{plugin_step_name} - Start''')
            try:
                list_urls = []
                _plugin_result = import_layer_1_module.init_url()
                if _plugin_result and isinstance(list(_plugin_result), list):
                    list_urls = list(_plugin_result)
            except Exception as e:
                # LOGGER.other_op.warning(
                #     f'''配置表sourceid：{sourceid} - {FIRST_LAYER_URL_GENERATE_FLAG} - {task_type} - 插件处理报错：{plugin_step_name} - {str(traceback.format_exc())}''')
                raise Exception(
                    f'''配置表sourceid：{sourceid} - {FIRST_LAYER_URL_GENERATE_FLAG} - {task_type} - 插件处理报错：{plugin_step_name} - {str(traceback.format_exc())}''')
            LOGGER.other_op.info(
                f'''配置表sourceid：{sourceid} - {FIRST_LAYER_URL_GENERATE_FLAG} - {task_type} - 插件处理：{plugin_step_name} - End''')
        # 插件处理 - END
        else:
            # 不存在init_url插件时，走框架默认的处理
            list_urls = self.gen_url_list(layer_0_first_url, layer_0_headers, layer_0_post_data,
                                          layer_0_page_style, layer_0_charset, test_param)
            # 开始执行after_init_url插件 - START
            plugin_after_step_name = 'after_init_url'
            if import_layer_1_module and hasattr(import_layer_1_module,
                                                 plugin_after_step_name) and callable(
                getattr(import_layer_1_module, plugin_after_step_name)):
                LOGGER.other_op.info(
                    f'''配置表sourceid：{sourceid} - {FIRST_LAYER_URL_GENERATE_FLAG} - {task_type} - 插件处理：{plugin_after_step_name} - Start''')
                try:
                    _plugin_result = import_layer_1_module.after_init_url(list_urls)
                    # _plugin_result存在是空列表的情况，比如都被插件代码过滤了，所以不需要判空
                    if isinstance(list(_plugin_result), list):
                        list_urls = list(_plugin_result)
                except Exception as e:
                    # LOGGER.other_op.warning(
                    #     f'''配置表sourceid：{sourceid} - {FIRST_LAYER_URL_GENERATE_FLAG} - {task_type} - 插件处理报错：{plugin_after_step_name} - {str(traceback.format_exc())}''')
                    raise Exception(
                        f'''配置表sourceid：{sourceid} - {FIRST_LAYER_URL_GENERATE_FLAG} - {task_type} - 插件处理报错：{plugin_after_step_name} - {str(traceback.format_exc())}''')
                LOGGER.other_op.info(
                    f'''配置表sourceid：{sourceid} - {FIRST_LAYER_URL_GENERATE_FLAG} - {task_type} - 插件处理：{plugin_after_step_name} - End''')
            # 插件处理 - END

        LOGGER.other_op.info(
            f"配置表sourceid：{sourceid} - {FIRST_LAYER_URL_GENERATE_FLAG} - {task_type} - 成功生成初始网址{len(list_urls)}条")
        if test_param and test_param['task_type'] == 'test':
            listResults = []
            for list_url in list_urls:
                if not list_url:
                    continue
                if isinstance(list_url, list) or isinstance(list_url, tuple):
                    a = {"_url": list_url[0] + '#post#=' + list_url[1], "current_layer": 1,
                         "total_layers": layers}
                else:
                    a = {"_url": list_url, "current_layer": 1, "total_layers": layers}
                listResults.append(a)
            return listResults
        else:
            for list_url in list_urls:
                if isinstance(list_url, tuple):
                    list_url = list_url[0] + '#post#=' + list_url[1]
                # 只有最后一级网址需要去重，初始网址无需去重
                # new_dict = {"sourceid": str(sourceid),
                #             "grabdatacode": str(record['grabdatacode']),
                #             "url": list_url,
                #             "queuename": queue_name}
                new_dict = {
                    "sourceid": str(sourceid),
                    "grabdatacode": str(record['grabdatacode']),
                    "modulecode": record.get('modulecode', ''),
                    "_url": list_url,
                    "current_layer": 1,
                    "total_layers": int(layers),
                    "task_type": test_param.get('task_type', ''),
                    "program_flag": FIRST_LAYER_URL_GENERATE_FLAG
                }
                # self.add_config(new_dict, record)
                if test_param and test_param['task_type'] == 'run':
                    queue_name = TEST_LC_JZ_DATA_TO_BE_DOWNLOADED_QUEUE_NAME
                else:
                    if db_env == '185':
                        queue_name = TEST_LC_JZ_DATA_TO_BE_DOWNLOADED_QUEUE_NAME
                    elif db_env == 'formal':
                        queue_name = LC_JZ_DATA_TO_BE_DOWNLOADED_QUEUE_NAME
                    else:
                        raise Exception(
                            f'配置表sourceid:{sourceid} - {FIRST_LAYER_URL_GENERATE_FLAG} - {task_type} - 网址:{list_url} - {new_dict} - 报错：未知的运行环境：{db_env}')
                    # queue_name = LC_JZ_DATA_TO_BE_DOWNLOADED_QUEUE_NAME

                try:
                    new_dict['send_to_next_mq'] = queue_name
                    new_dict['msg_create_time'] = datetime.datetime.now().strftime(
                        '%Y-%m-%d %H:%M:%S')
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
                        f'''配置表sourceid：{sourceid} - {FIRST_LAYER_URL_GENERATE_FLAG} - {task_type} - 网址:{list_url} - {new_dict} - init_url消息推送至{queue_name}成功！''')
                except Exception as e:
                    # LOGGER.other_op.info(
                    #     f'''配置表sourceid：{sourceid} - {FIRST_LAYER_URL_GENERATE_FLAG} - {task_type} - 网址:{list_url} - {new_dict} - init_url消息推送至{queue_name}失败！- {e}''')
                    raise Exception(
                        f'''配置表sourceid：{sourceid} - {FIRST_LAYER_URL_GENERATE_FLAG} - {task_type} - 网址:{list_url} - {new_dict} - init_url消息推送至{queue_name}失败！- {str(traceback.format_exc())}''')

        try:
            if import_layer_1_module:
                del sys.modules[current_import_module_name]
        except Exception as e:
            LOGGER.warning(
                f'配置表sourceid：{sourceid} - {FIRST_LAYER_URL_GENERATE_FLAG} - {task_type} - 生成初始网址程序 - 删除导入的插件模块失败，不影响抓取数据操作：{str(traceback.format_exc())}')


def consume_callback(ch, method, properties, body):
    # print('ch:', ch)
    # print('method:', method)
    # print('properties:', properties)
    # print('body', body)

    body = body.decode('utf-8')
    record = json.loads(body)
    # record = json.dumps(body, ensure_ascii=False)
    for k, v in record.items():
        try:
            if v is None:
                record[k] = ''
            if type(v) == str:
                if k == 'header':
                    pattern = re.compile(
                        "[\u4e00-\u9fa5]")  # Unicode范围内的汉字编码
                    s = v.encode('latin1').decode('gbk')
                    if re.search(pattern, s) is not None:
                        record[k] = s
        except Exception as e:
            LOGGER.other_op.warning(str(traceback.format_exc()))
    try:
        gen_handler = FirstLayerUrlGenerator()
        gen_handler.run(record)
    except Exception as e:
        LOGGER.other_op.warning(f'消息消费失败：{str(traceback.format_exc())},消息体：{record}')
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    else:
        ch.basic_ack(delivery_tag=method.delivery_tag)


def first_layer_url_generator(test_param=None):
    gen_handler = FirstLayerUrlGenerator()
    # 配置表提取语句
    if test_param:
        sourceid = test_param['sourceid']
        get_nav_config_sql = f'''SELECT * FROM RReportTask..ct_finprod_nav_config WITH(NOLOCK) WHERE sourceid='{sourceid}' '''
        records = PY_MSSQL.query(get_nav_config_sql)
        if records:
            for k, v in records[0].items():
                try:
                    if v is None:
                        records[0][k] = ''
                    if type(v) == str:
                        if k == 'header':
                            pattern = re.compile("[\u4e00-\u9fa5]")  # Unicode范围内的汉字编码
                            s = v.encode('latin1').decode('gbk')
                            if re.search(pattern, s) is not None:
                                records[0][k] = s
                        if k == 'layer_0_plugin':
                            pattern = re.compile("[\u4e00-\u9fa5]")  # Unicode范围内的汉字编码
                            s = v.encode('latin1').decode('gbk')
                            if re.search(pattern, s) is not None:
                                records[0][k] = s
                except Exception as e:
                    LOGGER.other_op.error(str(traceback.format_exc()))
            return_info = gen_handler.run(records[0], test_param)
            return return_info
    else:
        listener.start()  # 启动发送线程
        mac_ip = OhMyExtractor.get_ip()
        assert mac_ip
        LOGGER.other_op.debug(f'00 - 当前macip:{mac_ip}')
        LOGGER.other_op.info("00 - 生成初始网址程序启动!")

        if db_env == '185':
            queue_name = TEST_LC_JZ_CRON_TASK_QUEUE_NAME
        elif db_env == 'formal':
            queue_name = LC_JZ_CRON_TASK_QUEUE_NAME
        else:
            raise Exception(f'未知的运行环境：{db_env}')
        try:
            global MQ_Manager
            if not MQ_Manager:
                MQ_Manager = RabbitMqManager(mq_env)
            MQ_Manager.queue_declare(queue_name=queue_name, x_max_priority=10,
                                     prefetch_count=1, bind_dead_queue=True)
            MQ_Manager.consume(queue_name=queue_name, prefetch_count=1,
                               on_message_callback=consume_callback)
            while 1:
                try:
                    listener.start()  # 启动发送线程
                    MQ_Manager.start_consume()
                except Exception as e:
                    LOGGER.other_op.warning(
                        f"检测到rabbit mq 链接丢失，尝试重新链接：{str(traceback.format_exc())}")
        except Exception as e:
            LOGGER.other_op.warning(f"消息队列连接失败！ - {str(traceback.format_exc())}")


def run_formal_spider_conf(source_id_list: list):
    run_success_list = []
    if source_id_list:
        formal_sender = RabbitMqManager('formal')
        # formal_sender = RabbitMqManager('test')
        formal_sender.queue_declare(queue_name=LC_JZ_CRON_TASK_QUEUE_NAME,
                                    x_max_priority=10,
                                    prefetch_count=1,
                                    bind_dead_queue=True)
        try:
            for sourceid in source_id_list:
                try:
                    get_nav_config_sql = f'''select * FROM RReportTask..ct_finprod_nav_config  WHERE sourceid='{sourceid}' and state=1 and use_rules!=0 order by id desc '''
                    records = PY_MSSQL.query(get_nav_config_sql)
                    if records:
                        for k, v in records[0].items():
                            try:
                                if v is None:
                                    records[0][k] = ''
                                if type(v) == str:
                                    if k == 'header':
                                        pattern = re.compile("[\u4e00-\u9fa5]")  # Unicode范围内的汉字编码
                                        s = v.encode('latin1').decode('gbk')
                                        if re.search(pattern, s) is not None:
                                            records[0][k] = s
                                    if k == 'layer_0_plugin':
                                        pattern = re.compile("[\u4e00-\u9fa5]")  # Unicode范围内的汉字编码
                                        s = v.encode('latin1').decode('gbk')
                                        if re.search(pattern, s) is not None:
                                            records[0][k] = s
                            except Exception as e:
                                LOGGER.other_op.error(str(traceback.format_exc()))
                        record = records[0]
                        if record.get("tmstamp", None):
                            del record['tmstamp']
                        if record.get("entrydate", None):
                            del record['entrydate']
                        if record.get("entrytime", None):
                            del record['entrytime']

                        if record.get('state', None) != 1:
                            LOGGER.other_op.info(
                                f'''配置表sourceid：{sourceid} - 手动运行正式环境数据抓取 - 报错：当前任务未升级，无法运行！''')
                            continue

                        try:
                            record["program_flag"] = DIRECT_RUN_FORMAL_SPIDER_FLAG
                            record['send_to_next_mq'] = LC_JZ_CRON_TASK_QUEUE_NAME
                            record['msg_create_time'] = datetime.datetime.now().strftime(
                                '%Y-%m-%d %H:%M:%S')
                            record = json.dumps(record, ensure_ascii=False)
                            formal_sender.send_json_msg(queue_name=LC_JZ_CRON_TASK_QUEUE_NAME,
                                                        json_str=record,
                                                        priority=record.get('msg_priority', 0))
                            LOGGER.other_op.info(
                                f'''配置表sourceid：{sourceid} - 手动运行正式环境数据抓取 - 消息推送至{LC_JZ_CRON_TASK_QUEUE_NAME}成功！''')
                            run_success_list.append(sourceid)
                        except Exception as e:
                            LOGGER.other_op.info(
                                f'''配置表sourceid：{sourceid} - 手动运行正式环境数据抓取 - 消息推送至{LC_JZ_CRON_TASK_QUEUE_NAME}失败！- {str(traceback.format_exc())}''')
                except Exception as e:
                    LOGGER.other_op.warning(
                        f'''配置表sourceid：{sourceid} - 手动运行正式环境数据抓取 - 失败! - {str(traceback.format_exc())}''')
        except Exception as e:
            LOGGER.other_op.warning(
                f'''手动运行正式环境数据抓取 - 失败! - {str(traceback.format_exc())}''')
        finally:
            formal_sender.close()
        LOGGER.other_op.info(
            f'''手动运行正式环境数据抓取成功的配置表sourceid一共{len(run_success_list)}个：{run_success_list}''')


if __name__ == "__main__":
    first_layer_url_generator(test_param=None)
    # go_layer_0_p(
    #     test_param={"sourceid": "69066503-C605-3F5F-A62D-05B69B60C6C6", "task_type": "test"})
