#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Project ：  spider-nav-config-platform2 
@File    :   file_extract_threads.py
@Time    :   2024/4/29 8:43
@Author  :   MengJia_Fang 501738
@E-mail  :   fangmj@finchina.com
@License :   (C)Copyright Financial China Information & Technology Co., Ltd.
@Desc    :   从附件待抽取消息队列获取到数据，进行附件自动抽取，抽取完将抽取结果推送回待入库消息队列
@Version :   1.0
"""
import datetime
import json
import os
import random
import re
import sys
from urllib.parse import urlparse

parent_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(parent_path)

import traceback
from urllib import parse
import requests
from utils.jz_utils import get_file_path_from_module, create_data_guid
from spider_logging.kafka_logging import start_hear_beat, listener
from spider_logging.kafkalogger import KafkaLogger
import utils.env
import utils.settings
from utils.mqUtil import RabbitMqManager
from utils.spider_constant import TEST_LC_JZ_File_TO_BE_EXTRACT_QUEUE_NAME, \
    LC_JZ_File_TO_BE_EXTRACT_QUEUE_NAME, FILE_EXTRACT_FLAG, SUFFIX_DICT, \
    SAVE_ATTACHMENT_DOWNLOAD_PATH, LC_JZ_DATA_TO_BE_DEDUPLICATED_QUEUE_NAME, \
    TEST_LC_JZ_DATA_TO_BE_DEDUPLICATED_QUEUE_NAME
from utils.tools import OhMyExtractor

LOG_APP_ID = ''
LOGGER_NAME = 'spider.file_auto_extract'
LOGGER = KafkaLogger(name=LOGGER_NAME, appid=LOG_APP_ID)
start_hear_beat(print_console=True, app_id=LOG_APP_ID)

# 数据库环境 test/formal
db_env = utils.env.DB_ENV
# 消息队列环境 test/formal
mq_env = utils.env.MQ_ENV
MQ_Manager = ''


class FileExtract:
    def run(self, record):
        if not record.get('grabdatacode', '') or record['grabdatacode'] == 'None':
            LOGGER.set_data_source_id('0', durable=True)
        else:
            LOGGER.set_data_source_id(record['grabdatacode'], durable=True)
        # # set_origin_msg_time只有入库的时候需要打印
        # originTime1 = datetime.datetime.now().strftime("%Y-%m-%d")
        # originTime2 = datetime.datetime.now().strftime("%H:%M:%S")
        #
        # LOGGER.set_origin_msg_time(f'{originTime1} {originTime2}.000')
        sourceid = record['sourceid']
        stored_field = record.get('stored_fields', {})
        data_guid = record.get('data_guid', '')
        modulecode = record.get('modulecode', '')
        task_type = record.get('task_type', '')
        if task_type == 'test':
            if not stored_field:
                LOGGER.other_op.warning(
                    f'配置表sourceid:{sourceid} - {FILE_EXTRACT_FLAG}- {task_type} - {data_guid} - 报错 - 待抽取附件字段stored_fields不存在!')
                raise Exception(
                    f'配置表sourceid:{sourceid} - {FILE_EXTRACT_FLAG}- {task_type} - {data_guid} - 报错 - 待抽取附件字段stored_fields不存在!')
        else:
            if not (data_guid and stored_field):
                # LOGGER.other_op.warning(
                #     f'配置表sourceid:{sourceid} - {FILE_EXTRACT_FLAG}- {record.get("task_type","")} - {data_guid} - {stored_field} - 报错 - 数据guid或者待抽取附件字段stored_field不存在!')
                raise Exception(
                    f'配置表sourceid:{sourceid} - {FILE_EXTRACT_FLAG}- {task_type} - {data_guid} - 报错 - 数据guid或者待抽取附件字段stored_fields不存在!')
        content_is_auto_file = record.get('content_is_auto_file', None)
        # 满足条件的需要进行附件的抽取
        if content_is_auto_file and stored_field and stored_field.get('_auto_file_extract_content',
                                                                      ''):
            LOGGER.other_op.info(
                f'配置表sourceid:{sourceid} - {FILE_EXTRACT_FLAG}- {task_type} - {data_guid} - 附件抽取开始!')
            if not modulecode:
                if task_type == 'test':
                    LOGGER.other_op.warning(
                        f'配置表sourceid:{sourceid} - {FILE_EXTRACT_FLAG}- {task_type} - {data_guid} - 报错：modulecode'
                        f'字段不存在，无法获取对应的附件抽取逻辑!')
                raise Exception(
                    f'配置表sourceid:{sourceid} - {FILE_EXTRACT_FLAG}- {task_type} - {stored_field} - '
                    f'报错：modulecode字段不存在，无法获取对应的附件抽取逻辑!')
            # 抽取附件
            base_url = stored_field.get('_auto_file_extract_complete_url', '')
            if not base_url:
                base_url = stored_field.get('url', '')
            file_info_dict = self.file_extract(stored_field['_auto_file_extract_content'], base_url)

            if task_type == 'test':
                if not file_info_dict:
                    LOGGER.other_op.info(
                        f'配置表sourceid:{sourceid} - {FILE_EXTRACT_FLAG} - {task_type} - {data_guid} - 没有抽取到附件，测试数据接口，不保存html到公网!')
                    LOGGER.other_op.info(
                        f'配置表sourceid:{sourceid} - {FILE_EXTRACT_FLAG} - {task_type} - {data_guid} - 附件抽取结束 - 未抽取到附件!')
                    return record.get('stored_fields', {})
                else:
                    LOGGER.other_op.info(
                        f'配置表sourceid:{sourceid} - {FILE_EXTRACT_FLAG} - {task_type} - {data_guid} - 附件抽取结束!')
                    return self.handle_extracted_file_html_content_by_module_test_data(modulecode,
                                                                                       stored_field[
                                                                                           '_auto_file_extract_content'],
                                                                                       file_info_dict,
                                                                                       record)

            else:
                if not file_info_dict:
                    LOGGER.other_op.info(
                        f'配置表sourceid:{sourceid} - {FILE_EXTRACT_FLAG} - {task_type} - {data_guid} - 没有抽取到附件，保存html到公网!')

                    # 没抽取到附件有的模块是需要保存html到公网为止的
                    self.handle_no_file_html_content_by_module(modulecode,
                                                               stored_field[
                                                                   '_auto_file_extract_content'],
                                                               record)
                else:
                    LOGGER.other_op.info(
                        f'配置表sourceid:{sourceid} - {FILE_EXTRACT_FLAG} - {task_type} - {data_guid} - 抽取到附件{len(file_info_dict.keys())}条!')
                    # 组装抽取到的附件信息，再发送到入库队列去
                    self.handle_extracted_file_html_content_by_module(modulecode, stored_field[
                        '_auto_file_extract_content'], file_info_dict, record)
                LOGGER.other_op.info(
                    f'配置表sourceid:{sourceid} - {FILE_EXTRACT_FLAG} - {task_type} - {data_guid} - 附件抽取结束!')

    def file_extract(self, auto_file_extract_content, base_url):
        file_info_list = re.finditer(
            r"""(?i)<a[^<>]*?\s(href|HREF)\s*?=\s*?('|")(.+?)("|')[^<>]*?>([\S\s]*?)<\\*/(a|A)>""",
            auto_file_extract_content)
        # img_info_list = re.finditer(r"""(?i)<img[^<>]*?\s(src|SRC)\s*?=\s*?(\\)*('|")(http[^'"]*?)(\\)*("|')[^<>]*?>([\S\s]*?)<\\*/(img|IMG)>""",auto_file_extract_content)
        # file_url需要去重
        deduplicate_file_url_dict = {}
        if file_info_list:
            for file_info in file_info_list:  # 3  5
                file_url = file_info.group(3)
                file_url_name = file_info.group(5)
                if file_url_name or file_url:
                    file_url_name = re.sub('\s', '', re.sub('<[^<>]*?>', '', file_url_name))
                    file_url = re.sub('&amp;', '&', file_url)
                    file_url = re.sub('(\s|%20)*?$', '', file_url)
                    if re.search(r'(?i)(htm|html|\.cn|\.cn/|\.com|\.com/)$',
                                 file_url) or file_url.startswith(
                        r'https://integrity.finchina.com'):
                        continue
                    file_url = parse.unquote(file_url)
                    if ('\\' in file_url):
                        file_url = file_url.replace('\\', '/')
                    file_url = self.complete_url(file_url, base_url)
                    # 判断filename的长度，优先取filename更长的
                    if file_url in deduplicate_file_url_dict:
                        deduplicate_file_url_dict[file_url] = file_url_name if len(
                            file_url_name) > len(deduplicate_file_url_dict[file_url]) else \
                            deduplicate_file_url_dict[file_url]
                    else:
                        deduplicate_file_url_dict[file_url] = file_url_name
        file_info_dict = {}
        if deduplicate_file_url_dict:
            # 返回的file_info_dict是抽取到的所有：附件链接:附件名称，key-value塞入到字典中
            file_info_dict = self.check_url_is_file(deduplicate_file_url_dict, base_url)
        return file_info_dict

    @staticmethod
    def judge_is_file_from_manually_mark(file_url):
        # 特殊字段包含 判断为附件
        is_file_flag = False
        # 注意要小写
        is_file_str_list = [r'?fileurl=', r'resource/download?id=',
                            r'publicannouncement.do?method=downfile&sendid=']
        for is_file_str in is_file_str_list:
            if is_file_str in file_url.lower():
                is_file_flag = True
                break
        return is_file_flag

    @staticmethod
    def customize_suffix(url):
        """自定义后缀名

        Args:
            url (str): 附件或者图片的链接

        Returns:
            srt: 附件后缀
        """
        if url.startswith(r"https://credit.hanzhong.gov.cn/publicity/img.png?id="):
            return r".png"
        elif re.search(r"http://nmg.tzxm.gov.cn/indexlink/bagzs/\d+\.jspx", url):
            return r".html"
        return ""

    def get_suffix_file(self, file, use_type):
        """通过附件url和附件名称来获取后缀

        Args:
            file (str): 附件url或者附件名称

        Returns:
            [type]: 附件后缀
            :param file:
            :param use_type:
        """
        suffix = os.path.splitext(file)
        suffix = re.sub(r"&.*$", "", suffix[-1]).lower()
        if self.suffix_is_true(suffix, use_type):
            return suffix
        else:
            return ''

    @staticmethod
    def suffix_is_true(suffix, use_type):
        """判断后缀是否对

        Args:
            suffix (_type_): _description_
            use_type (_type_): _description_

        Returns:
            _type_: _description_
        """
        if not suffix:
            return False
        if 3 <= len(suffix) <= 6:
            if use_type == 0:
                for suf in [r".html", r"htm", r".html", r'.cn', r'.com', r'.jspx']:
                    if suf in suffix:
                        return False
                if suffix.startswith(r"htm#"):
                    return False
            return True
        else:
            return False

    @staticmethod
    def dict_lower(dict_info):
        """将字典的键 都变成小写

        Args:
            dict_info (dict): _description_

        Returns:
            dict: 小写后的字典
        """
        new_dict = {}
        for i, j in dict_info.items():
            new_dict[i.lower()] = j
        return new_dict

    def content_type(self, headers_dict):
        # 通过 Content-Type 判断后缀名
        suffix = ''
        headers_dict = self.dict_lower(headers_dict)
        ContentType = headers_dict.get('content-type')
        if ContentType:
            ContentType = re.sub(r"\s", "", ContentType)
            ContentType_s = ContentType.split(";")
            for key in SUFFIX_DICT.keys():
                if key.lower() in ContentType_s:
                    suffix = SUFFIX_DICT.get(key.lower())
                    break
        return suffix

    def cont_dispos(self, headers_dict, use_type):
        # 通过 Content-Disposition 判断后缀名
        suffix = ''
        headers_dict = self.dict_lower(headers_dict)
        ContDispos = headers_dict.get('content-disposition')
        if ContDispos:
            ContDispos = re.sub(r"['\"\s]", "", ContDispos.lower())
            ContDispos = re.sub(r"；", ";", ContDispos)
            ContDispos_list = ContDispos.split(";")
            for _i in ContDispos_list:
                _a = _i.split("=")
                if len(_a) == 2 and _a[0].lower() in ['filename', 'filename*']:
                    suffix = self.get_suffix_file(_a[1], use_type)
        return suffix

    def get_suffix_headers(self, fileurl, headers_dict, use_type):
        # 通过headers识别
        if not headers_dict:
            r = requests.head(fileurl, timeout=7)
            headers_dict = dict(r.headers)
        suffix = self.content_type(headers_dict)
        if not self.suffix_is_true(suffix, use_type):
            suffix = self.cont_dispos(headers_dict, use_type)
        if self.suffix_is_true(suffix, use_type):
            return suffix
        else:
            return ""

    def get_file_suffix(self, fileurl, filename='', headers_dict={}, use_type=0):
        """获取后缀名

        Args:
            fileurl (str): 附件url
            filename (str, optional): 附件名称，[description]. Defaults to ''.
            headers_dict (dict, optional): 附件请求头 [description]. Defaults to {}.
            use_type (int, optional): 0：默认，1：会将text/html识别成.html

        Returns:
            str: 后缀名 eg: .pdf
        """

        suffix = self.customize_suffix(fileurl)
        if suffix or r'.jixnfile' in filename or r'.htm' in filename:
            use_type = 1
        if use_type == 1:
            SUFFIX_DICT["text/html"] = ".html"
        # 通过url来获取后缀
        if not suffix:
            suffix = self.get_suffix_file(fileurl, use_type)
        if not suffix:
            # 通过headers识别
            suffix = self.get_suffix_headers(fileurl, headers_dict, use_type)
        if not suffix and filename:
            suffix = self.get_suffix_file(filename, use_type=1)
            if suffix in [r'.html', r'.htm']:
                return suffix
        if self.suffix_is_true(suffix, use_type):
            # print(f"获取后缀成功：{suffix}")
            return suffix
        else:
            # print(f"获取后缀失败")
            return ''

    def check_url_is_file(self, deduplicate_file_url_dict, base_url):
        new_file_info_dict = {}
        if deduplicate_file_url_dict:
            for new_file_url in deduplicate_file_url_dict.keys():
                new_file_name = deduplicate_file_url_dict[new_file_url]
                sign = re.search(
                    r'(?i)(\.xls|\.xlsx|\.pdf|\.zip|\.csv|\.txt|\.rar|\.wps|\.tif|\.docx|\.doc|\.gif|\.jpg|\.bmp|\.tiff|\.png|\.jpeg|\.XLS|\.XLSX|\.PDF|\.ZIP|\.CSV|\.TXT|\.RAR|\.WPS|\.DOCX|\.DOC|\.GIF|\.JPG|\.BMP|\.TIFF|\.PNG|\.JPEG|\.et|\.swf)',
                    new_file_url)
                sign1 = re.search(
                    r'(?i)(\.xls|\.xlsx|\.pdf|\.zip|\.csv|\.txt|\.rar|\.wps|\.tif|\.docx|\.doc|\.gif|\.jpg|\.bmp|\.tiff|\.png|\.jpeg|\.XLS|\.XLSX|\.PDF|\.ZIP|\.CSV|\.TXT|\.RAR|\.WPS|\.DOCX|\.DOC|\.GIF|\.JPG|\.BMP|\.TIFF|\.PNG|\.JPEG|\.et|\.swf|\.jixnfile)',
                    new_file_name)
                if sign or sign1:
                    new_file_info_dict[new_file_url] = new_file_name
                else:
                    # 特殊的直接判断为附件
                    is_file_flag = self.judge_is_file_from_manually_mark(new_file_url)
                    if is_file_flag:
                        new_file_info_dict[new_file_url] = new_file_name
                        continue
                    # 发送请求
                    headers = {
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                        "Accept-Language": "zh-CN,zh;q=0.8",
                        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36 SE 2.X MetaSr 1.0"
                    }
                    i = 1
                    while i <= 3:
                        list_proxies = ['10.17.205.91:808', '10.17.205.96:808', '10.17.206.27:808',
                                        '10.17.206.28:808', '', '']
                        _http = re.match('http(s)?', new_file_url).group()
                        proxy = random.choice(list_proxies)
                        proxies = {_http: proxy}
                        try:
                            if db_env in ['formal_yun', 'test_yun']:
                                res = requests.head(url=new_file_url, headers=headers, timeout=8)
                            else:
                                res = requests.head(url=new_file_url, headers=headers,
                                                    proxies=proxies, timeout=8)
                        except Exception as e:
                            i = i + 1
                        else:
                            break
                    if i > 3:
                        # 不符合附件网址情况
                        continue
                    suffix = self.get_file_suffix(new_file_url, new_file_name, dict(res.headers))
                    if suffix:
                        new_file_info_dict[new_file_url] = new_file_name
        return new_file_info_dict

    def handle_no_file_html_content_by_module(self, modulecode, html_content, record):
        if modulecode == 14:
            # 保存没抽取到附件的正文字段到公网
            self.handle_no_file_html_content_by_module_14(modulecode, html_content, record)
        if modulecode in (15, 16):
            # 保存抽取附件的正文字段到数据库字段
            self.handle_no_file_html_content_by_module_15(modulecode, html_content, record)
        else:
            pass

    def handle_extracted_file_html_content_by_module(self, modulecode, html_content, file_info_dict,
                                                     record):
        if modulecode == 14:
            self.handle_extracted_file_html_content_by_module_14(html_content, file_info_dict,
                                                                 record)
        if modulecode in (15, 16):
            self.handle_extracted_file_html_content_by_module_15(html_content, file_info_dict,
                                                                 record)
        else:
            pass

    def handle_extracted_file_html_content_by_module_test_data(self, modulecode, html_content,
                                                               file_info_dict, record):
        if modulecode == 14:
            return self.handle_extracted_file_html_content_by_module_14_test_data(html_content,
                                                                                  file_info_dict,
                                                                                  record)
        if modulecode in (15, 16):
            return self.handle_extracted_file_html_content_by_module_15_test_data(html_content,
                                                                                  file_info_dict,
                                                                                  record)
        else:
            return []

    def handle_extracted_file_html_content_by_module_14(self, html_content, file_info_dict, record):
        for key in file_info_dict.keys():
            new_record = {}
            new_record.update(record)

            # stored_fields添加进去附件信息
            new_stored_field = {}
            new_stored_field.update(record["stored_fields"])
            new_stored_field['url'] = key
            # new_stored_field['cgrec1'] = file_info_dict[key]

            new_record["stored_fields"] = new_stored_field
            # 抽取完后的数据的content_is_auto_file必须设置为空，防止无限重复抽取
            new_record['content_is_auto_file'] = None

            # 重新生成guid
            data_guid = create_data_guid(new_record["sourceid"], new_record['dup_rule'],
                                         new_record["stored_fields"], LOGGER_NAME,
                                         LOG_APP_ID, FILE_EXTRACT_FLAG)
            new_record['data_guid'] = data_guid

            if db_env == '185':
                queue_name = TEST_LC_JZ_DATA_TO_BE_DEDUPLICATED_QUEUE_NAME
            elif db_env == 'formal':
                queue_name = LC_JZ_DATA_TO_BE_DEDUPLICATED_QUEUE_NAME

            else:
                raise Exception(
                    f'配置表sourceid:{new_record["sourceid"]} - {FILE_EXTRACT_FLAG} - {new_record["data_guid"]} - 报错：未知的运行环境：{db_env}')
            self.send_msg_to_queue(queue_name, new_record)

    @staticmethod
    def send_msg_to_queue(queue_name, record):
        try:
            record['send_to_next_mq'] = queue_name
            record["program_flag"] = FILE_EXTRACT_FLAG
            record['msg_create_time'] = datetime.datetime.now().strftime(
                '%Y-%m-%d %H:%M:%S')
            record['msg_priority'] = record.get('msg_priority', 0)
            second_msg = json.dumps(record, ensure_ascii=False)
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
                f'''配置表sourceid:{record["sourceid"]} - {FILE_EXTRACT_FLAG} - {record["data_guid"]} - 消息推送至{queue_name}成功！''')
        except Exception as e:
            LOGGER.other_op.info(
                f'''配置表sourceid:{record["sourceid"]} - {FILE_EXTRACT_FLAG} - {record["data_guid"]} - 报错：消息推送至{queue_name}失败！- {str(traceback.format_exc())}''')
            raise Exception(
                f'''配置表sourceid:{record["sourceid"]} - {FILE_EXTRACT_FLAG} - {record["data_guid"]} - 报错：消息推送至{queue_name}失败！- {str(traceback.format_exc())}''')

    def handle_no_file_html_content_by_module_14(self, modulecode, html_content, record):
        try:
            file_path = get_file_path_from_module(SAVE_ATTACHMENT_DOWNLOAD_PATH, modulecode,
                                                  record["sourceid"])
            file_path = file_path + rf'\{record["sourceid"]}_{record["data_guid"]}.html'
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(html_content)

            # 将已保存的html文件路径塞回去入库
            record["stored_fields"]['filepath'] = file_path
            # 抽取完后的数据的content_is_auto_file必须设置为空，防止无限重复抽取
            record['content_is_auto_file'] = None

            if db_env == '185':
                queue_name = TEST_LC_JZ_DATA_TO_BE_DEDUPLICATED_QUEUE_NAME
            elif db_env == 'formal':
                queue_name = LC_JZ_DATA_TO_BE_DEDUPLICATED_QUEUE_NAME
            else:
                raise Exception(
                    f'配置表sourceid:{record["sourceid"]} - {FILE_EXTRACT_FLAG} - {record["data_guid"]} - 报错：未知的运行环境：{db_env}')
            self.send_msg_to_queue(queue_name, record)
        except Exception as e:
            LOGGER.other_op.warning(
                f'配置表sourceid：{record["sourceid"]} - {FILE_EXTRACT_FLAG} - 数据guid:{record["data_guid"]} - 保存html附件至公网目录 - 异常 - {str(traceback.format_exc())}')

        else:
            LOGGER.other_op.info(
                f'配置表sourceid：{record["sourceid"]} - {FILE_EXTRACT_FLAG} - 数据guid:{record["data_guid"]} - 保存html附件至公网目录 - 成功')

    def handle_no_file_html_content_by_module_15(self, modulecode, html_content, record):
        try:
            # 将已保存的html文件路径塞回去入库
            record["stored_fields"]['content'] = html_content
            # 抽取完后的数据的content_is_auto_file必须设置为空，防止无限重复抽取，现在抽取改到数据抽取完发送，应该不存在无限重复抽取了，但是保险起见
            record['content_is_auto_file'] = None

            if db_env == '185':
                queue_name = TEST_LC_JZ_DATA_TO_BE_DEDUPLICATED_QUEUE_NAME
            elif db_env == 'formal':
                queue_name = LC_JZ_DATA_TO_BE_DEDUPLICATED_QUEUE_NAME
            else:
                raise Exception(
                    f'配置表sourceid:{record["sourceid"]} - {FILE_EXTRACT_FLAG} - {record["data_guid"]} - 报错：未知的运行环境：{db_env}')
            self.send_msg_to_queue(queue_name, record)
        except Exception as e:
            LOGGER.other_op.warning(
                f'配置表sourceid：{record["sourceid"]} - {FILE_EXTRACT_FLAG} - 数据guid:{record["data_guid"]} - 保存html附件至公网目录 - 异常 - {str(traceback.format_exc())}')

        else:
            LOGGER.other_op.info(
                f'配置表sourceid：{record["sourceid"]} - {FILE_EXTRACT_FLAG} - 数据guid:{record["data_guid"]} - 保存html附件至公网目录 - 成功')

    @staticmethod
    def complete_url(url, base_url):
        if url and not re.match('^http[s]*?', url):
            while url.startswith('.'):
                url = re.sub('^\.', '', url)
            while url.startswith('/'):
                url = re.sub('^/', '', url)
            base_url_m = re.match('^(.*?)#file_url#(.*?)$', base_url)
            if base_url_m:
                base_url_list = base_url_m.groups()
                if base_url_list[0]:
                    url_prefix = base_url_list[0] + '/' if not base_url_list[0].endswith('/') else \
                        base_url_list[0]
                    url = url_prefix + url
                if base_url_list[1]:
                    url = url + base_url_list[1]
            else:

                url = r'://'.join(
                    [urlparse(base_url).scheme, urlparse(base_url).netloc]) + '/' + url
        return url

    @staticmethod
    def handle_extracted_file_html_content_by_module_14_test_data(html_content, file_info_dict,
                                                                  record):
        new_stored_field_list = []
        for key in file_info_dict.keys():
            new_stored_field = {}
            new_stored_field.update(record["stored_fields"])
            new_stored_field['url'] = key
            # new_stored_field['cgrec1'] = file_info_dict[key]

            new_stored_field_list.append(new_stored_field)
        return new_stored_field_list

    @staticmethod
    def handle_extracted_file_html_content_by_module_15_test_data(html_content, file_info_dict,
                                                                  record):
        new_stored_field_list = []
        for key in file_info_dict.keys():
            new_stored_field = {}
            new_stored_field.update(record["stored_fields"])
            new_stored_field['downfileurl'] = key
            new_stored_field['downfilename'] = file_info_dict[key]

            new_stored_field_list.append(new_stored_field)
        return new_stored_field_list

    def handle_extracted_file_html_content_by_module_15(self, html_content, file_info_dict, record):
        for key in file_info_dict.keys():
            new_record = {}
            new_record.update(record)

            # stored_fields添加进去附件信息
            new_stored_field = {}
            new_stored_field.update(record["stored_fields"])
            new_stored_field['downfileurl'] = key
            new_stored_field['downfilename'] = file_info_dict[key]

            new_record["stored_fields"] = new_stored_field
            # 抽取完后的数据的content_is_auto_file必须设置为空，防止无限重复抽取
            new_record['content_is_auto_file'] = None

            # 重新生成附件guid，银行公告模块式是入一条公告表，入多条附件表
            sub_data_guid = create_data_guid(new_record["sourceid"], new_record['multiguidrule'],
                                             new_record["stored_fields"], LOGGER_NAME,
                                             LOG_APP_ID, FILE_EXTRACT_FLAG)
            new_record['sub_data_guid'] = sub_data_guid

            if db_env == '185':
                queue_name = TEST_LC_JZ_DATA_TO_BE_DEDUPLICATED_QUEUE_NAME
            elif db_env == 'formal':
                queue_name = LC_JZ_DATA_TO_BE_DEDUPLICATED_QUEUE_NAME

            else:
                raise Exception(
                    f'配置表sourceid:{new_record["sourceid"]} - {FILE_EXTRACT_FLAG} - 主表data_guid:{new_record["data_guid"]} - 从表data_guid:{new_record["sub_data_guid"]} - 报错：未知的运行环境：{db_env}')
            self.send_msg_to_queue(queue_name, new_record)


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
        file_extract_ = FileExtract()
        file_extract_.run(body_dict)
    except Exception as e:
        LOGGER.other_op.warning(f'消息消费失败：{str(traceback.format_exc())},消息体：{body_temp}')
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    else:
        ch.basic_ack(delivery_tag=method.delivery_tag)
        LOGGER.receive_message.info(f"00 - [tag]{method.delivery_tag} -消息已确认消费")


def file_extract(test_param=None):
    # 配置表提取语句
    if test_param and test_param['task_type'] == 'test':
        _file_extract = FileExtract()
        re_info = _file_extract.run(test_param)
        return re_info
    else:
        listener.start()  # 启动发送线程
        macip = OhMyExtractor.get_ip()
        assert macip
        LOGGER.other_op.debug(f'00 - 当前macip:{macip}')
        LOGGER.other_op.info("00 - 附件自动抽取程序启动!")

        if db_env == '185':
            queue_name = TEST_LC_JZ_File_TO_BE_EXTRACT_QUEUE_NAME
        elif db_env == 'formal':
            queue_name = LC_JZ_File_TO_BE_EXTRACT_QUEUE_NAME
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
            while 1:
                try:
                    listener.start()  # 启动发送线程
                    MQ_Manager.start_consume()
                except Exception as e:
                    LOGGER.other_op.warning("检测到rabbit mq 链接丢失，尝试重新链接")
        except Exception as e:
            LOGGER.other_op.warning(f"消息队列连接失败！ - {e}")


if __name__ == '__main__':
    file_extract()
