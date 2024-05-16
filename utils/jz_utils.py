#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Project ：  spider-nav-config-platform 
@File    :   jz_utils.py
@Time    :   2024/2/21 17:50
@Author  :   MengJia_Fang 501738
@E-mail  :   fangmj@finchina.com
@License :   (C)Copyright Financial China Information & Technology Co., Ltd.
@Desc    :   Description
@Version :   1.0
"""
import datetime
import importlib
import importlib.util
import importlib.abc
import subprocess
import pkg_resources
import math
import pathlib
import re
import sys
import traceback

import requirements
import toml
from spider_logging.kafkalogger import KafkaLogger

from utils import spider_constant
from utils.datecal import datecal
from utils.reqUtil import OhMyReq
from utils.spider_constant import SAVE_SOURCE_CODE_FILE_PATH, FILTER_DEAD_MSG_CONFIG_FILE_PATH
from utils.tablename import get_module_zh_name
from utils.tools import OhMyExtractor


def create_data_guid(sourceid, dup_rule, result, log_name, log_app_id, program_flag=''):
    thread_logger = KafkaLogger(name=log_name, appid=log_app_id)
    # 获取生成guid的参数，以前叫去重参数字段
    if not isinstance(dup_rule, str):
        raise Exception(f'dup_rule不是str类型：{dup_rule}')
    dup_rule = dup_rule.replace('，', ',')
    dup_rule_list = dup_rule.split(',')

    # 生成数据的唯一guid
    dup_text = ''
    dup_success = True
    dup_success_message = ''

    # 校验生成guid的参数是否存在，字段转小写再比对
    dict_lower = [i.lower() for i in list(result.keys())]
    for dup_field in dup_rule_list:
        if dup_field.lower() in dict_lower:
            if result.get(dup_field, None):
                dup_text += result[dup_field]
        else:
            dup_success = False
            dup_success_message = f',guid生成参数中的字段不在抓取结果中 - {dup_rule} - {dict_lower}'
    if not dup_success:
        thread_logger.de_duplication.error(
            f'配置表sourceid：{sourceid} - {program_flag} - 生成数据guid失败:{dup_success_message}')
        raise Exception(
            f'配置表sourceid：{sourceid} - {program_flag} - 生成数据guid失败:{dup_success_message}')

    dup_text = 'http://' + dup_text
    data_guid = OhMyExtractor.generate_uniqueGuid(dup_text, '_url')

    return data_guid


def get_cookie_from_file(cookie_path, log_name, log_app_id):
    thread_logger = KafkaLogger(name=log_name, appid=log_app_id)

    try:
        cookie_path = spider_constant.COOKIE_BASE_DIR + '\\' + cookie_path
        public_c_p = pathlib.Path(cookie_path)
        if public_c_p.exists():
            with open(cookie_path, 'rt', encoding='UTF-8-sig') as f:
                cookie = f.read()
            if cookie:
                return cookie
            else:
                thread_logger.other_op.error(f'【获取的公网cookie为空】：{cookie_path}')
        else:
            thread_logger.other_op.error(f'【公网cookie文件不存在】：{cookie_path}')
    except Exception as e:
        thread_logger.other_op.exception(e)
    else:
        thread_logger.other_op.info(f'【获取cookie成功】：{cookie}')


def get_url_from_file(file_path, log_name, log_app_id):
    thread_logger = KafkaLogger(name=log_name, appid=log_app_id)
    url_list = []
    try:
        file_path = spider_constant.URL_FROM_FILE_BASE_DIR + '\\' + file_path
        public_f_p = pathlib.Path(file_path)
        if public_f_p.exists():
            with open(file_path, 'rt', encoding='UTF-8-sig') as f:
                urls = f.readlines()
            if urls:
                url_list = list(urls)
            else:
                thread_logger.other_op.error(f'【未获取到公网指定url文件路径中的url】：{file_path}')
        else:
            thread_logger.other_op.error(f'【公网url文件不存在】：{file_path}')
    except Exception as e:
        thread_logger.other_op.exception(e)

    thread_logger.other_op.info(f'【获取公网url成功】：共{len(urls)}条网址')
    return url_list


def get_all_pages(ori_str, first_url, headers, charset, test_param, log_name, log_app_id,
                  first_url_res_type=''):
    """
    @ori_str：配置了包含#page=1,3,1#配置的需要分页的字符串
    @test_param：用于判断当前是什么环境：测试验证接口、测试运行接口、正式运行接口（测试验证接口只返回5页用于查看，其他接口均返回全部分页后的数据列表）
    @log_name：用于打日志
    @log_app_id：用于打日志
    """
    data_list = list()
    thread_logger = KafkaLogger(name=log_name, appid=log_app_id)
    try:
        if not isinstance(ori_str, str):
            return data_list
        if ori_str and re.findall('#page=date=(.+?)#', ori_str):
            date_str_temp = ori_str
            date_reg_str = re.findall('#page=date=(.+?)#', date_str_temp)[0]
            date_reg_list = date_reg_str.split('||')
            if date_reg_list:
                # 最后根据date_fmt_str的格式生成当前日期
                date_reg_fmt_str = date_reg_list[0]
                date_reg_param_str = date_reg_list[1] if len(date_reg_list) >= 2 else ''
                if date_reg_param_str:
                    # 目前分页日期只支持一种日期变化
                    # date_reg_param_list = date_reg_param_str.split('*')
                    day_m = re.match('^day([+-])(.+)$', date_reg_param_str)
                    month_m = re.match('^month([+-])(.+)$', date_reg_param_str)
                    year_m = re.match('^year([+-])(.+)$', date_reg_param_str)
                    if day_m:
                        op = day_m.groups()[0]
                        day = int(day_m.groups()[1])
                        if first_url:
                            start = 1
                        else:
                            start = 0
                        for d in range(start, day + 1):
                            now = datetime.datetime.now()
                            now = now + datetime.timedelta(days=eval(op + str(d)))
                            date_str = now.strftime(date_reg_fmt_str)
                            data_list.append(re.sub('#page=date=.+?#', date_str, date_str_temp))
                    elif month_m:
                        op = month_m.groups()[0]
                        month = int(month_m.groups()[1])
                        if first_url:
                            start = 1
                        else:
                            start = 0
                        for d in range(start, month + 1):
                            now = datetime.datetime.now()
                            now = now.replace(
                                month=eval('now.month' + eval(op + str(d))))
                            date_str = now.strftime(date_reg_fmt_str)
                            data_list.append(re.sub('#page=date=.+?#', date_str, date_str_temp))
                    elif year_m:
                        op = month_m.groups()[0]
                        year = int(month_m.groups()[1])
                        if first_url:
                            start = 1
                        else:
                            start = 0
                        for d in range(start, year + 1):
                            now = datetime.datetime.now()
                            now = now.replace(
                                year=eval('now.year' + eval(op + str(d))))
                            date_str = now.strftime(date_reg_fmt_str)
                            data_list.append(re.sub('#page=date=.+?#', date_str, date_str_temp))
            else:
                now = datetime.datetime.now()
                date_str = now.strftime(date_reg_str)
                data_list.append(re.sub('#page=date=.+?#', date_str, date_str_temp))
        else:
            if ori_str and re.findall('#page=(.+?)#', ori_str):
                str_temp = ori_str
                pages = re.findall('#page=(.+?)#', str_temp)[0]
                if ori_str.startswith('http') and re.search('\(', pages):
                    # 目前不支持post参数分页的参数从第一页网址中获取
                    rule = re.search('(\([\s\S]*?\)),([^,]*?),([^,]*?)$', pages)
                    if rule:
                        pages = [rule.group(1), rule.group(2), rule.group(3)]
                        if re.search('\(', pages[0]):
                            if not first_url:
                                # 默认从1开始
                                first_url = re.sub('\#page=.+?\#', '1', str_temp)
                            max_page = get_page_param_from_web(first_url, headers, '', charset,
                                                               pages[0])
                            pages[0] = max_page
                        if re.search('\(', pages[1]):
                            if not first_url:
                                # 默认从1开始
                                first_url = re.sub('\#page=.+?\#', '1', str_temp)
                            max_page = get_page_param_from_web(first_url, headers, '', charset,
                                                               pages[1])
                            pages[1] = max_page
                        pages = list(map(lambda x: int(x), pages[:3]))
                else:
                    pages = pages.split(',')
                    pages = list(map(lambda x: int(x), pages))
                if len(pages) == 3:
                    if (pages[0] > pages[1]) and pages[2] < 0:
                        # 倒序
                        if (pages[0] - pages[1] + 1) > 5 and test_param and test_param[
                            "task_type"] == 'test':
                            for i in range(pages[0], pages[0] + (pages[2] * 5), pages[2]):
                                data_list.append(re.sub('\#page=.+?\#', str(i), str_temp))
                        else:
                            for i in range(pages[0], pages[1] - 1, pages[2]):
                                data_list.append(re.sub('\#page=.+?\#', str(i), str_temp))
                    else:
                        # 正序
                        if (pages[1] - pages[0] + 1) > 5 and test_param and test_param[
                            "task_type"] == 'test':
                            for i in range(pages[0], pages[0] + (pages[2] * 5), pages[2]):
                                data_list.append(re.sub('\#page=.+?\#', str(i), str_temp))

                        else:
                            for i in range(pages[0], pages[1] + 1, pages[2]):
                                data_list.append(re.sub('\#page=.+?\#', str(i), str_temp))
    except Exception as e:
        thread_logger.other_op.warning(f'生成根据分页参数生成多页数据报错：{ori_str}')
        thread_logger.other_op.error(f'{str(traceback.format_exc())}')
    return data_list


def extract_gen_url_list(url, headers=None, postdata=None, pagestyle=None, charset=None,
                         test_param=None, log_name=None, log_app_id=None):
    # list_urls = []
    # 该字段用于post data分页
    post_data_list = []
    # 该字段用于网址url分页
    url_list = []
    # 返回的网址列表
    list_urls = []

    # 处理分页：post分页和网址分页
    if postdata:
        url_date_post_data = re.search('{(year|day)((-|\+)(\d+))?}', postdata)
        if url_date_post_data:
            postdata = datecal(postdata, url_date_post_data[1], url_date_post_data[3],
                               url_date_post_data[4])

        # post参数是否存在分页：post_data_list不为空就是存在分页
        post_data_list = list(
            get_all_pages(postdata, '', headers, charset, test_param, log_name, log_app_id))
    else:
        postdata = ''

    if pagestyle:
        url_date_page_style = re.search(r'{(year|day)((-|\+)(\d+))?}', str(pagestyle))
        if url_date_page_style:
            pagestyle = datecal(pagestyle, url_date_page_style[1], url_date_page_style[3],
                                url_date_page_style[4])
        # 网址需要分页
        url_list2 = list(
            get_all_pages(pagestyle, '', headers, charset, test_param, log_name, log_app_id))
        url_list.extend(url_list2)

    if post_data_list and url_list:
        raise Exception(
            f'正文post参数和网址同时存在分页参数，目前不支持，请检查-正文网址和正文post参数是否配置错误')
    elif url_list:
        # url_list.insert(0, url)
        list_urls = url_list
    elif post_data_list:
        if url:
            # post分页的时候必须传入url，因为要拼接网址
            list_urls = [(url, p) for p in post_data_list]
    elif postdata:
        if url:
            list_urls = [(url, postdata)]
    else:
        if not url:
            list_urls = [pagestyle]
        else:
            list_urls = [url]

    return list_urls


def get_page_param_from_web(url, headers, postdata, charset, pagematch):
    """
    :function: 获取最大页码
    :return: str
    """
    max_page = 0
    REQ = OhMyReq()
    try:
        if isinstance(url, str):
            # get
            try:
                resource = REQ.get_html(url=url, headers=headers, postdata=postdata,
                                        charset=charset)
            except Exception as e:
                error_flag = 1

        elif isinstance(url, tuple):
            # post
            try:
                resource = REQ.get_html(url=url[0], postdata=url[1], headers=headers)
            except Exception as e:
                error_flag = 1
        else:
            resource = {'res_code': 'ERROR'}
    except:
        pass
    else:
        if '/' in pagematch:

            match_list = pagematch.split('/')
            total = re.search(match_list[0].replace(r'(', ''), resource['resource'])
            total = re.sub('\D', '', total[0])
            max_page = math.ceil(int(total) / int(match_list[1].replace(r')', '')))
        else:
            max_page = re.search(pagematch, resource['resource'])
            max_page = re.sub('\D', '', max_page[0])

    finally:
        return max_page


def get_file_path_from_module(base_path, module_code, sourceid, layer='', url=''):
    file_path = ''
    p = pathlib.Path(base_path)
    if not p.exists():
        raise Exception(f'业务模块编码为：{module_code}，最外层目录不存在:{base_path}，需要自行手动创建')
    module_name = get_module_zh_name(int(module_code))

    if module_name:
        if not (layer and url):
            file_path = SAVE_SOURCE_CODE_FILE_PATH + rf'\{module_name}\{sourceid}\{datetime.datetime.now().strftime("%Y")}\{datetime.datetime.now().strftime("%m")}\{datetime.datetime.now().strftime("%d")}'
            file_final_p = pathlib.Path(file_path)
            if not file_final_p.parent.parent.parent.parent.parent.exists():
                file_final_p.parent.parent.parent.parent.parent.mkdir()
            if not file_final_p.parent.parent.parent.parent.exists():
                file_final_p.parent.parent.parent.parent.mkdir()
            if not file_final_p.parent.parent.parent.exists():
                file_final_p.parent.parent.parent.mkdir()
            if not file_final_p.parent.parent.exists():
                file_final_p.parent.parent.mkdir()
            if not file_final_p.parent.exists():
                file_final_p.parent.mkdir()
        else:
            # file_path = SAVE_SOURCE_CODE_FILE_PATH + rf'\{module_name}\{sourceid}\{datetime.datetime.now().strftime("%Y")}\{datetime.datetime.now().strftime("%m")}\{datetime.datetime.now().strftime("%Y-%m-%d")}\第{layer}层\{sourceid + "_" + datetime.datetime.now().strftime("%Y-%m-%d-%H")}_{OhMyExtractor.generate_uniqueGuid(url, "_url")}.html'
            file_path = SAVE_SOURCE_CODE_FILE_PATH + rf'\{module_name}\{sourceid}\{datetime.datetime.now().strftime("%Y")}\{datetime.datetime.now().strftime("%m")}\{datetime.datetime.now().strftime("%d")}\第{layer}层\{sourceid + "_" + datetime.datetime.now().strftime("%Y-%m-%d")}_{OhMyExtractor.generate_uniqueGuid(url, "_url")}.html'
            file_final_p = pathlib.Path(file_path)
            if not file_final_p.parent.parent.parent.parent.parent.parent.exists():
                file_final_p.parent.parent.parent.parent.parent.parent.mkdir()
            if not file_final_p.parent.parent.parent.parent.parent.exists():
                file_final_p.parent.parent.parent.parent.parent.mkdir()
            if not file_final_p.parent.parent.parent.parent.exists():
                file_final_p.parent.parent.parent.parent.mkdir()
            if not file_final_p.parent.parent.parent.exists():
                file_final_p.parent.parent.parent.mkdir()
            if not file_final_p.parent.parent.exists():
                file_final_p.parent.parent.mkdir()
            if not file_final_p.parent.exists():
                file_final_p.parent.mkdir()

    return file_path


def handle_date_fmt(date_str: str):
    if not date_str:
        return date_str
    try:
        orig_date_reg_list = re.findall(r'\#date\=(.+?)\#', date_str)
        if orig_date_reg_list:
            date_str_temp = date_str
            for date_reg_str in orig_date_reg_list:
                _final_date_str = ''
                date_reg_list = date_reg_str.split('||')
                if date_reg_list:
                    # 最后根据date_fmt_str的格式生成当前日期
                    date_reg_fmt_str = date_reg_list[0]
                    now = datetime.datetime.now()
                    date_reg_param_str = date_reg_list[1] if len(date_reg_list) >= 2 else ''
                    if date_reg_param_str:
                        date_reg_param_list = date_reg_param_str.split('*')
                        if date_reg_param_list:
                            for date_reg_param in date_reg_param_list:
                                day_m = re.match('^day(.+)$', date_reg_param)
                                month_m = re.match('^month(.+)$', date_reg_param)
                                year_m = re.match('^year(.+)$', date_reg_param)
                                if day_m:
                                    now = now + datetime.timedelta(days=eval(day_m.groups()[0]))
                                elif month_m:
                                    now = now.replace(
                                        month=eval('now.month' + eval('month_m.groups()[0]')))
                                elif year_m:
                                    now = now.replace(
                                        year=eval('now.year' + eval('year_m.groups()[0]')))

                    _final_date_str = now.strftime(date_reg_fmt_str)
                else:
                    now = datetime.datetime.now()
                    _final_date_str = now.strftime(date_reg_str)

                date_str_temp = date_str_temp.replace(f'#date={date_reg_str}#', _final_date_str)
            date_str = date_str_temp
    except Exception as e:
        raise Exception(f'处理日期格式异常：{str(traceback.format_exc())}')
    return date_str


def install_package(package_name):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package_name])


def import_module_from_str(module_name, module_code, log_name, log_app_id):
    thread_logger = KafkaLogger(name=log_name, appid=log_app_id)
    try:
        # 解析模块代码以获取依赖信息（此处假设模块代码包含标准的`requirements.txt`格式的注释）
        requirements_comment = "# Requirements:"
        requirements_start = module_code.find(requirements_comment)
        if requirements_start != -1:
            requirements_start += len(requirements_comment)
            requirements_end = module_code.find("\n", requirements_start)
            requirements_str = module_code[requirements_start:requirements_end].strip()
            requirements_str_list = requirements_str.split(',')

            # 使用requirements-parser库解析依赖列表
            # parser = RequirementsParser()
            # requirements.parser()
            dependencies = set()
            for req_str in requirements_str_list:
                for req in requirements.parse(req_str):
                    dependencies.add(req.name)
            # dependencies = {req.name for req in requirements.parse(requirements_str_list[0])}

            # 检查并安装缺失的依赖
            installed_packages = {pkg.project_name.lower() for pkg in pkg_resources.working_set}
            missing_deps = dependencies - installed_packages
            for dep in missing_deps:
                install_package(dep)

        # # 创建一个空的模块对象
        # memory_module = types.ModuleType(module_name)
        #
        # # 将模块代码作为字符串执行，相当于在模块内部执行这段代码
        # exec(module_code, memory_module.__dict__)
        # print(memory_module.init_url())
        # return memory_module

        # 创建一个加载器以从字符串中加载模块代码
        class StringLoader(importlib.abc.Loader):
            def exec_module(self, module):
                exec(module_code, module.__dict__)

        # 创建一个规范
        spec = importlib.util.spec_from_loader(module_name, StringLoader())

        # 使用规范来创建模块
        dynamic_module = importlib.util.module_from_spec(spec)

        # 将模块添加到 sys.modules 中
        sys.modules[module_name] = dynamic_module

        # 执行模块
        spec.loader.exec_module(dynamic_module)

        # 返回导入的模块
        return dynamic_module

    except Exception as e:
        thread_logger.other_op.warning(f"导入插件模块报错 - 无法加载动态模块:{str(traceback.format_exc())}")
        raise Exception(f"导入插件模块报错 - 无法加载动态模块:{str(traceback.format_exc())}")
        # return None


def get_dead_msg_config_from_toml(config_key):
    sourceid_list = []
    dead_msg_ttl_for_sourceid_list = []
    dead_msg_ttl = ''
    try:
        with open(FILTER_DEAD_MSG_CONFIG_FILE_PATH, "r", encoding="utf-8") as f:
            data = toml.load(f)
        if data and data.get(config_key, ''):
            sourceid_list = data[config_key].get('sourceid_list', [])
            dead_msg_ttl = data[config_key].get('dead_msg_ttl', "")
            dead_msg_ttl_for_sourceid_list = data[config_key].get('dead_msg_ttl_for_sourceid_list',
                                                                  [])
    except Exception as e:
        raise Exception(f"获取死信队列消息处理配置失败：{str(traceback.format_exc())}")
    return sourceid_list, dead_msg_ttl_for_sourceid_list, dead_msg_ttl


def check_msg_is_expire(date_str: str, _ttl_str: str):
    discard_flag = False
    if not _ttl_str:
        return discard_flag
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    date1 = datetime.datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
    date2 = datetime.datetime.strptime(now, '%Y-%m-%d %H:%M:%S')
    delta = date2 - date1
    m = re.match('^([\d]+)([a-zA-Z])$', _ttl_str)
    if not m:
        return discard_flag
    if m.groups()[1] == 'D' and delta.days >= int(m.groups()[0]):
        discard_flag = True
    elif m.groups()[1] == 'h' and (delta.seconds // 3600 >= int(m.groups()[0])):
        discard_flag = True
    elif m.groups()[1] == 'm' and (delta.seconds // 60 >= int(m.groups()[0])):
        discard_flag = True
    elif m.groups()[1] == 's' and delta.seconds >= int(m.groups()[0]):
        discard_flag = True

    return discard_flag
