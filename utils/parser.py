#!/usr/bin/env python
# -*- encoding: utf-8 -*-
"""
@Project ：  spider-nav-config-platform
@File    :   data_stored_threads.py
@Time    :   2024/4/01 13:44
@Author  :   MengJia_Fang 501738
@E-mail  :   fangmj@finchina.com
@License :   (C)Copyright Financial China Information & Technology Co., Ltd.
@Desc    :   Description
@Version :   1.0
"""

import json
import logging
import re
import time
import traceback

from bs4 import BeautifulSoup
from lxml import etree


def get_record(rules, list_record):
    if rules:
        records = list_record
        # 2022-10-19增加
        if re.search(r'^@$', rules):
            pass
        else:
            levels = rules.split('@')
            for level in levels:
                # 2022-01-04 拓展json抽取语法
                if re.match(r'[\s\S]*\[\d+\]$', level):
                    index = int(re.search(r'\[(\d+)\]$', level).group(1))
                    level = re.search(r'^([\s\S]*)\[\d+\]$', level).group(1)
                    records = records.get(level)
                    records = records[index]
                else:
                    records = records.get(level)
        return records
    else:
        return list_record


class Extraction(object):
    def __init__(self, text):
        self.text = text
        self.root_node = None

    # 按xpath抽取,传入规则字典
    def extraction_by_xpath(self, xpath):
        # 首先转换根节点
        if self.root_node is None:
            try:
                self.root_node = etree.HTML(str(self.text))
                # html = etree.tostring(self.root_node, encoding="utf-8").decode("utf-8")
            except Exception:
                soup = BeautifulSoup(self.text, 'html5lib')
                fixed_html = soup.prettify()
                self.root_node = etree.HTML(str(fixed_html))
        nodes = self.root_node.xpath(xpath)
        try:
            content_list = [
                etree.tostring(n, encoding="utf-8").decode("utf-8")
                for n in nodes
            ]
        except Exception as e:
            # print(e)
            content_list = nodes
        finally:
            return content_list

    # 根据xpath剔除节点
    def eliminate_by_xpath(self, xpath):
        # 首先转换根节点
        if self.root_node is None:
            try:
                self.root_node = etree.HTML(str(self.text))
            except Exception:
                soup = BeautifulSoup(self.text, 'html5lib')
                fixed_html = soup.prettify()
                self.root_node = etree.HTML(str(fixed_html))
        nodes = self.root_node.xpath(xpath)
        for n in nodes:
            n.getparent().remove(n)
        self.text = etree.tostring(self.root_node, encoding="utf-8").decode("utf-8")

    # 按正则抽取
    def extraction_by_all(self, pattern):
        p = re.compile(pattern)
        result = re.findall(p, self.text)
        return result


class Parser(object):
    """
    @function: 解析类
    """

    def __init__(self, content, rules):
        self.content = content
        self.rules = rules
        self.content_obj = Extraction(content)
        self.remove_tag = re.compile(r'<[^>]+>', re.S)
        self.remove_blank = re.compile(r'^\s+|\s+$', re.S)
        self.remove_special = re.compile(r'&#[\s\S]*?;', re.S)
        self.logger = logging.getLogger("spider.parser")

    # 按正则过滤字段
    def filter_rule_by_all(self, filter_field, field_obj):
        filter_flag = 0
        field = filter_field.get("field", '')
        if isinstance(field_obj, str):
            field_obj = {field: field_obj}
        try:
            filter_rule = filter_field.get("filter", None)
            if filter_rule:
                if re.match('^\=\>必须为空\<\=$', filter_rule):
                    if field_obj.get(f'{field}', ''):
                        filter_flag = 1
                    return filter_flag
                if re.match('^\=\>不能为空\<\=$', filter_rule):
                    if not field_obj.get(f'{field}', ''):
                        filter_flag = 1
                    return filter_flag

                filter_rules = r'(' + filter_rule + ')'
                if not re.search(filter_rules, field_obj.get(f'{field}', '')):
                    self.logger.debug(f'过滤该条记录 - 字段{field} - 不包含{filter_rule}')
                    filter_flag = 1
                    return filter_flag
            filter_rule = filter_field.get("nofilter", None)
            if filter_rule:
                filter_rules = r'(' + filter_rule + ')'
                if re.search(filter_rules, field_obj.get(f'{field}', '')):
                    self.logger.debug(f'过滤该条记录 - 字段{field} - 包含{filter_rule}')
                    filter_flag = 1
                    return filter_flag
        except Exception as e:
            self.logger.debug(f'字段{field} -过滤出错！- {str(traceback.format_exc())}')
        finally:
            return filter_flag

    # 解析html
    def extraction_html(self, all_field_obj, all_field_name=None, current_spider_url=''):
        """
        @all_field_obj：所有层级已抓取字段的key-value dict
        @all_field_name：所有层级已抓取字段的key list
        """
        assert self.rules
        extract_results = []
        # 预先剔除源码的规则集合
        source_code_eliminate_rule_list = []
        # 抽取_content字段用于循环的规则集合
        _content_extract_rule_list = []
        # 抽取其他字段的规则集合
        others_extract_rule_list = []
        # 字段的替换规则集合
        others_replace_rule_list = []

        self.rules = json.loads(self.rules)
        # 规则分类
        for rule_dict in self.rules:
            # 遍历规则列表
            # 字段名
            field = rule_dict["field"]
            # 用于标识当前规则是用于数据提取还是替换
            useful = rule_dict["useful"]
            # 规则类型
            rule_type = rule_dict["rule_type"]

            if field in all_field_name:
                raise Exception(f'数据抽取报错：{field}字段抽取规有误，字段名称不可重复，请检查')

            if not field and not useful:
                # 剔除网页源码
                if rule_type == 'xpath':
                    source_code_eliminate_rule_list.insert(0, rule_dict)
                elif rule_type == 'regEx':
                    source_code_eliminate_rule_list.insert(-1, rule_dict)
            elif field == '_content' and useful:
                # 抽取用于循环的默认字段
                _content_extract_rule_list.append(rule_dict)
            elif field and useful:
                # 抽取其他字段
                others_extract_rule_list.append(rule_dict)
            elif field and not useful:
                #  # 替换字段
                others_replace_rule_list.append(rule_dict)
            else:
                raise Exception(f'数据抽取报错：{field}字段抽取规则有误，请检查')

        # 剔除网页源码中的不需要部分
        if source_code_eliminate_rule_list:
            for rule_dict in source_code_eliminate_rule_list:
                rule_type = rule_dict["rule_type"]
                rule_str = rule_dict["rule"]
                replace = rule_dict.get("replace", None)
                if not replace:
                    replace = ''
                try:
                    if rule_type == 'xpath':
                        self.content_obj.eliminate_by_xpath(rule_str)
                    elif rule_type == 'regEx':
                        self.content_obj.text = (
                            lambda x: re.sub(rule_str, replace, x))(
                            self.content_obj.text)
                except Exception as e:
                    raise Exception(f'数据抽取报错：剔除源码出错，请检查 - {str(traceback.format_exc())}')
        # 去掉源码中的注释部分
        # self.content_obj.text = re.sub(r'<!--[\s\S]*?-->', r'', self.content_obj.text)

        # 开始抽取_content
        loop_records = []
        if _content_extract_rule_list:
            for rule_dict in _content_extract_rule_list:
                rule_type = rule_dict["rule_type"]
                rule_str = rule_dict["rule"]
                try:
                    if rule_type == 'xpath':
                        loop_records = self.content_obj.extraction_by_xpath(
                            rule_str)
                    elif rule_type == 'regEx':
                        loop_records = self.content_obj.extraction_by_all(
                            rule_str)
                except Exception as e:
                    raise Exception(
                        f'数据抽取报错：抽取{rule_dict["field"]}字段出错，请检查规则：{rule_type}-{rule_str} - {str(traceback.format_exc())}')
                else:
                    # 过滤记录
                    temp_filter_records = loop_records[::-1]
                    for record in temp_filter_records:
                        # if not re.search(filter_rules, record):
                        #     loop_records.remove(record)
                        filter_flag = self.filter_rule_by_all(rule_dict, record)
                        if filter_flag == 1:
                            loop_records.remove(record)

                    # filter_rule = rule_dict.get("filter", None)
                    # if filter_rule:
                    #     filter_rules = r'(' + filter_rule + ')'
                    #     temp_filter_records = loop_records[::-1]
                    #     for record in temp_filter_records:
                    #         if not re.search(filter_rules, record):
                    #             loop_records.remove(record)
                    #
                    # no_filter_rule = rule_dict.get("nofilter", None)
                    # if no_filter_rule:
                    #     filter_rules = r'(' + filter_rule + ')'
                    #     temp_no_filter_records = loop_records[::-1]
                    #     for record in temp_no_filter_records:
                    #         if re.search(filter_rules, record):
                    #             loop_records.remove(record)

                # 替换
                if others_replace_rule_list:
                    for replace_field_rule_dict in others_replace_rule_list:
                        if replace_field_rule_dict.get('field', '') == '_content':
                            for index in range(len(loop_records)):
                                record_obj = Extraction(loop_records[index])
                                rule_type = replace_field_rule_dict["rule_type"]
                                rule_str = replace_field_rule_dict["rule"]
                                replace = replace_field_rule_dict["replace"]
                                if not replace:
                                    replace = ''
                                try:
                                    if rule_type == 'xpath':
                                        record_obj.eliminate_by_xpath(rule_str)
                                    elif rule_type == 'regEx':
                                        record_obj.text = (
                                            lambda x: re.sub(rule_str, replace, x))(
                                            record_obj.text)
                                except Exception as e:
                                    raise Exception(f'数据抽取报错：剔除字段content源码出错，请检查 - {str(traceback.format_exc())}')
                                else:
                                    loop_records[index] = record_obj.text
                                    # eliminate_list_others.remove(eliminate_field)

                                    # 过滤记录
                                    temp_filter_records = loop_records[::-1]
                                    for record in temp_filter_records:
                                        # if not re.search(filter_rules, record):
                                        #     loop_records.remove(record)
                                        filter_flag = self.filter_rule_by_all(rule_dict, record)
                                        if filter_flag == 1:
                                            loop_records.remove(record)

                                    # filter_rule = rule_dict.get("filter", None)
                                    # if filter_rule:
                                    #     filter_rules = r'(' + filter_rule + ')'
                                    #     for record in loop_records[::-1]:
                                    #         if not re.search(filter_rules, record):
                                    #             loop_records.remove(record)
                                    #
                                    # filter_rule = rule_dict.get("nofilter", None)
                                    # if filter_rule:
                                    #     filter_rules = r'(' + filter_rule + ')'
                                    #     for record in loop_records[::-1]:
                                    #         if re.search(filter_rules, record):
                                    #             loop_records.remove(record)

        # 抽取字段：对每个loop_record进行所有字段的抽取，结果保存在record_result
        if loop_records:
            for loop_record in loop_records:
                filter_flag = 0
                record_result = {}
                if isinstance(loop_record, tuple):
                    loop_record = str(loop_record[0])
                if not isinstance(loop_record, str):
                    loop_record = str(loop_record)

                if others_extract_rule_list:
                    con_record_obj = Extraction(loop_record)
                    for rule_dict in others_extract_rule_list:
                        field_value = []
                        field = rule_dict["field"]
                        rule_type = rule_dict["rule_type"]
                        rule_str = rule_dict["rule"]
                        # 判断字段是否需要直接从源码中取值
                        is_from_source_code = rule_dict.get('is_from_source_code', '')
                        try:
                            if is_from_source_code and is_from_source_code is True:
                                if rule_type == 'xpath':
                                    field_value = self.content_obj.extraction_by_xpath(
                                        rule_str)

                                elif rule_type == 'regEx':
                                    field_value = self.content_obj.extraction_by_all(
                                        rule_str)
                            else:
                                if rule_type == 'xpath':
                                    field_value = con_record_obj.extraction_by_xpath(
                                        rule_str)

                                elif rule_type == 'regEx':
                                    field_value = con_record_obj.extraction_by_all(
                                        rule_str)

                                elif rule_type == 'const':
                                    if rule_str == 'yyyy-mm-dd':
                                        rule_str = time.strftime("%Y-%m-%d", time.localtime())
                                        field_value.append(rule_str)
                                    else:
                                        field_value.append(rule_str)
                                elif rule_type == 'reference':
                                    merged_dict = {**record_result, **all_field_obj}
                                    if re.findall('\=\>(.+?)\<\=', rule_str):
                                        rule_str_temp = rule_str
                                        referenced_field_name_list = re.findall('\=\>(.+?)\<\=',
                                                                                rule_str_temp)
                                        for referenced_field_name in referenced_field_name_list:
                                            if merged_dict.get(referenced_field_name, None) is None:
                                                raise Exception(
                                                    f'数据抽取报错：字段{field}抽取出错，请检查-引用其他字段的顺序，被引用字段必须先配置')
                                            rule_str_temp = re.sub(
                                                fr'\=\>{referenced_field_name}\<\=',
                                                merged_dict[referenced_field_name], rule_str_temp)
                                        field_value = [rule_str_temp]
                                    else:
                                        if rule_str == '_from_current_layer_spider_url':
                                            field_value = [current_spider_url]
                                        else:
                                            if not merged_dict.get(rule_str, ''):
                                                raise Exception(
                                                    f'数据抽取报错：字段{field}抽取出错，请检查-引用其他字段的顺序，被引用字段必须先配置')
                                            field_value = [merged_dict[rule_str]]

                        except Exception as e:
                            raise Exception(f'数据抽取报错：字段{field}抽取出错，请检查 - {str(traceback.format_exc())} - {loop_record}')

                        if field_value:
                            # 去标签/前后空格
                            if len(field_value) > 1:
                                field_value_str = ''
                                for f_v in field_value:
                                    field_value_str += str(f_v)
                                if field_value_str:
                                    field_value = [field_value_str]
                            if field == '_auto_file_extract_content':
                                record_result[field] = field_value[0]
                                # ! 过滤记录
                                filter_flag = self.filter_rule_by_all(rule_dict,
                                                                      record_result[
                                                                          field])
                                if filter_flag == 1:
                                    record_result[field] = ''
                                    break
                            else:
                                field_result = self.remove_tag.sub('', str(field_value[0]))
                                field_result = self.remove_special.sub(' ', field_result)
                                field_result = field_result.replace('•', '·').replace('&#8226;',
                                                                                      '·').replace(
                                    '\u2022', '·').replace('\u2003', ' ').replace(u'\xa0',
                                                                                  ' ').replace(
                                    '\u0020', ' ')  # NBSP 占位符去除 20221117添加
                                record_result[field] = self.remove_blank.sub('', field_result)
                                # ! 过滤记录
                                filter_flag = self.filter_rule_by_all(rule_dict, field_result)
                                if filter_flag == 1:
                                    record_result[field] = ''
                                    break
                        else:
                            field_result = ''
                            record_result[field] = ''
                            # # ! 过滤记录
                            filter_flag = self.filter_rule_by_all(rule_dict, field_result)
                            if filter_flag == 1:
                                break

                    # 对抽取出来的字段结果进行替换操作
                    if filter_flag == 0:
                        if others_replace_rule_list:
                            for replace_field_rule_dict in others_replace_rule_list:
                                if replace_field_rule_dict['field'] != '_content':
                                    field_obj = Extraction(
                                        record_result.get(replace_field_rule_dict['field'], ''))
                                    rule_type = replace_field_rule_dict["rule_type"]
                                    rule_str = replace_field_rule_dict["rule"]
                                    replace = replace_field_rule_dict["replace"]
                                    if not replace:
                                        replace = ''
                                    try:
                                        if rule_type == 'xpath':
                                            field_obj.eliminate_by_xpath(rule_str)
                                        elif rule_type == 'regEx':
                                            field_obj.text = (
                                                lambda x: re.sub(rule_str, replace, x))(
                                                field_obj.text)
                                        elif rule_type == 'const':
                                            if field_obj.text == '':  # 为空则用默认值填充
                                                if rule_str == 'yyyy-mm-dd':
                                                    rule_str = time.strftime("%Y-%m-%d",
                                                                             time.localtime())
                                                field_obj.text = rule_str
                                        elif rule_type == 'reference':
                                            merged_dict2 = {**record_result, **all_field_obj}
                                            if re.findall('\=\>(.+?)\<\=', rule_str):
                                                rule_str_temp = rule_str
                                                referenced_field_name_list2 = re.findall(
                                                    '\=\>(.+?)\<\=', rule_str_temp)
                                                for referenced_field_name2 in referenced_field_name_list2:
                                                    if merged_dict2.get(referenced_field_name2,
                                                                        None) is None:
                                                        raise Exception(
                                                            f'数据抽取报错：字段{field}抽取出错，请检查-引用其他字段的顺序，被引用字段必须先配置')
                                                    rule_str_temp = re.sub(
                                                        fr'\=\>{referenced_field_name2}\<\=',
                                                        merged_dict2[referenced_field_name2],
                                                        rule_str_temp)
                                                rule_str = rule_str_temp
                                            else:
                                                if rule_str == '_from_current_layer_spider_url':
                                                    rule_str = current_spider_url
                                                else:
                                                    if not merged_dict2.get(rule_str, ''):
                                                        raise Exception(
                                                            f'数据抽取报错：字段{field}抽取出错，请检查-引用其他字段的顺序，被引用字段必须先配置')
                                                    rule_str = merged_dict2[rule_str]
                                            field_obj.text = rule_str

                                        # ! 过滤记录
                                        filter_flag = self.filter_rule_by_all(
                                            replace_field_rule_dict, field_obj.text)
                                        if filter_flag == 1:
                                            break
                                    except Exception as e:
                                        raise Exception(f'数据抽取报错：字段{field}抽取出错，请检查 - {str(traceback.format_exc())}')
                                    else:
                                        record_result[
                                            replace_field_rule_dict['field']] = field_obj.text

                if not filter_flag or filter_flag == 0:
                    record_result['record_html'] = str(loop_record)
                    extract_results.append(record_result)
        else:
            # 当没有配置_conetnt的时候，也支持所有字段都直接从源码抽取，但是此时抽取到的字段数据就不支持循环了，而是直接组装成一个dict塞到列表返回
            if (not _content_extract_rule_list) and others_extract_rule_list:
                from_source_extrat_dict = {}
                filter_flag = 0
                # is_from_source_code_flag = False
                for rule_dict in others_extract_rule_list:
                    field_value = []
                    field = rule_dict["field"]
                    rule_type = rule_dict["rule_type"]
                    rule_str = rule_dict["rule"]
                    # 判断字段是否需要直接从源码中取值
                    is_from_source_code = rule_dict.get('is_from_source_code', '')
                    if not is_from_source_code:
                        try:
                            if rule_type == 'const':
                                if rule_str == 'yyyy-mm-dd':
                                    field_value = [time.strftime("%Y-%m-%d",
                                                                 time.localtime())]
                                else:
                                    field_value = [rule_str]
                            elif rule_type == 'reference':
                                merged_dict2 = {**from_source_extrat_dict, **all_field_obj}
                                if re.findall('\=\>(.+?)\<\=', rule_str):
                                    rule_str_temp = rule_str
                                    referenced_field_name_list2 = re.findall(
                                        '\=\>(.+?)\<\=', rule_str_temp)
                                    for referenced_field_name2 in referenced_field_name_list2:
                                        if merged_dict2.get(referenced_field_name2, None) is None:
                                            raise Exception(
                                                f'数据抽取报错：字段{field}抽取出错，请检查-引用其他字段的顺序，被引用字段必须先配置')
                                        rule_str_temp = re.sub(
                                            fr'\=\>{referenced_field_name2}\<\=',
                                            merged_dict2[referenced_field_name2],
                                            rule_str_temp)
                                    rule_str = rule_str_temp
                                else:
                                    if rule_str == '_from_current_layer_spider_url':
                                        rule_str = current_spider_url
                                    else:
                                        if not merged_dict2.get(rule_str, ''):
                                            raise Exception(
                                                f'数据抽取报错：字段{field}抽取出错，请检查-引用其他字段的顺序，被引用字段必须先配置')
                                        rule_str = merged_dict2[rule_str]
                                field_value = [rule_str]
                            else:
                                continue
                        except Exception as e:
                            raise Exception(f'数据抽取时报错：字段{field}抽取出错，请检查配置规则 - {str(traceback.format_exc())}')
                        # raise Exception(f'数据直接从源码抽取报错：字段{field}抽取时未获取到[直接从源码获取]的标识,请检查配置规则!')
                    else:
                        try:
                            # is_from_source_code_flag = True
                            if rule_type == 'xpath':
                                field_value = self.content_obj.extraction_by_xpath(
                                    rule_str)
                            elif rule_type == 'regEx':
                                field_value = self.content_obj.extraction_by_all(
                                    rule_str)
                        except Exception as e:
                            raise Exception(f'数据直接从源码抽取时报错：字段{field}抽取出错，请检查配置规则 - {str(traceback.format_exc())}')

                    if field_value:
                        # 去标签/前后空格
                        if len(field_value) > 1:
                            field_value_str = ''
                            for f_v in field_value:
                                field_value_str += str(f_v)
                            if field_value_str:
                                field_value = [field_value_str]
                        if field == '_auto_file_extract_content':
                            from_source_extrat_dict[field] = field_value[0]
                            # ! 过滤记录
                            filter_flag = self.filter_rule_by_all(rule_dict,
                                                                  from_source_extrat_dict[field])
                            if filter_flag == 1:
                                # from_source_extrat_dict[field] = ''
                                print(f'抽取到的数据为：{from_source_extrat_dict[field]}，已被过滤')
                                break
                        else:
                            field_result = self.remove_tag.sub('', str(field_value[0]))
                            field_result = self.remove_special.sub(' ', field_result)
                            field_result = field_result.replace('•', '·').replace('&#8226;',
                                                                                  '·').replace(
                                '\u2022', '·').replace('\u2003', ' ').replace(u'\xa0', ' ').replace(
                                '\u0020', ' ')  # NBSP 占位符去除 20221117添加
                            from_source_extrat_dict[field] = self.remove_blank.sub('', field_result)
                            # ! 过滤记录
                            filter_flag = self.filter_rule_by_all(rule_dict, field_result)
                            if filter_flag == 1:
                                # from_source_extrat_dict[field] = ''
                                print(f'抽取到的数据为：{from_source_extrat_dict[field]}，已被过滤')
                                break
                    else:
                        field_result = ''
                        from_source_extrat_dict[field] = ''
                        # # ! 过滤记录
                        filter_flag = self.filter_rule_by_all(rule_dict, field_result)
                        if filter_flag == 1:
                            print('抽取到的数据为空，已被过滤')
                            break

                # 对抽取出来的字段结果进行替换操作
                if filter_flag == 0:
                    if others_replace_rule_list:
                        for replace_field_rule_dict in others_replace_rule_list:
                            if replace_field_rule_dict['field'] != '_content':
                                field_obj = Extraction(
                                    from_source_extrat_dict.get(replace_field_rule_dict['field'],
                                                                ''))
                                rule_type = replace_field_rule_dict["rule_type"]
                                rule_str = replace_field_rule_dict["rule"]
                                replace = replace_field_rule_dict["replace"]
                                if not replace:
                                    replace = ''
                                try:
                                    if rule_type == 'xpath':
                                        field_obj.eliminate_by_xpath(rule_str)
                                    elif rule_type == 'regEx':
                                        field_obj.text = (
                                            lambda x: re.sub(rule_str, replace, x))(
                                            field_obj.text)
                                    elif rule_type == 'const':
                                        if field_obj.text == '':  # 为空则用默认值填充
                                            if rule_str == 'yyyy-mm-dd':
                                                rule_str = time.strftime("%Y-%m-%d",
                                                                         time.localtime())

                                            field_obj.text = rule_str
                                    elif rule_type == 'reference':
                                        merged_dict3 = {**from_source_extrat_dict, **all_field_obj}
                                        if re.findall('\=\>(.+?)\<\=', rule_str):
                                            rule_str_temp3 = rule_str
                                            referenced_field_name_list3 = re.findall(
                                                '\=\>(.+?)\<\=', rule_str_temp3)
                                            for referenced_field_name3 in referenced_field_name_list3:
                                                if merged_dict3.get(referenced_field_name3,
                                                                    None) is None:
                                                    raise Exception(
                                                        f'数据抽取报错：字段{replace_field_rule_dict["field"]}抽取出错，请检查-引用其他字段的顺序，被引用字段必须先配置')
                                                rule_str_temp3 = re.sub(
                                                    fr'\=\>{referenced_field_name3}\<\=',
                                                    merged_dict3[referenced_field_name3],
                                                    rule_str_temp3)
                                            rule_str = rule_str_temp3
                                        else:
                                            if rule_str == '_from_current_layer_spider_url':
                                                rule_str = current_spider_url
                                            else:
                                                if not merged_dict3.get(rule_str, ''):
                                                    raise Exception(
                                                        f'直接从源码抽取报错：字段{replace_field_rule_dict["field"]}抽取出错，请检查-引用其他字段的顺序，被引用字段必须先配置')
                                                rule_str = merged_dict3[rule_str]
                                        field_obj.text = rule_str

                                    # ! 过滤记录
                                    filter_flag = self.filter_rule_by_all(
                                        replace_field_rule_dict, field_obj.text)
                                    if filter_flag == 1:
                                        break
                                except Exception as e:
                                    raise Exception(
                                        f'数据抽取报错：字段{replace_field_rule_dict["field"]}抽取出错，请检查 - {str(traceback.format_exc())}')
                                else:
                                    from_source_extrat_dict[
                                        replace_field_rule_dict['field']] = field_obj.text

                if not filter_flag or filter_flag == 0 and from_source_extrat_dict:
                    from_source_extrat_dict['record_html'] = '直接从源码抽取，无中间循环记录'
                    extract_results.append(from_source_extrat_dict)
            else:
                raise Exception('数据抽取报错：未获取到_content记录，或者_content被过滤,请检查_content规则!')

        return extract_results


class ExtractionJson(object):
    def __init__(self, text):
        try:
            self.json_object = json.loads(text)
        except Exception as e:
            self.json_object = ''

    @property
    def text(self):
        return json.dumps(self.json_object)

    def replace(self, json_key, replace_str):
        if replace_str:
            self.json_object[json_key] = replace_str
        else:
            self.json_object.pop(json_key)

    def get_list(self, rules):
        if rules:
            records = self.json_object
            # 2022-10-19增加
            if re.search(r'^@$', rules):
                pass
            else:
                levels = rules.split('@')
                for level in levels:
                    # 2022-01-04 拓展json抽取语法
                    if re.match(r'[\s\S]*\[\d+\]$', level):
                        index = int(re.search(r'\[(\d+)\]$', level).group(1))
                        level = re.search(r'^([\s\S]*)\[\d+\]$', level).group(1)
                        records = records.get(level)
                        records = records[index]
                    else:
                        records = records.get(level)
            return records
        else:
            return self.json_object


class ParserJson(object):
    """
    @function: 解析JSON类
    """

    def __init__(self, content, rules):
        self.content = content
        self.rules = rules
        self.content_obj = ExtractionJson(content)
        self.remove_tag = re.compile(r'<[^>]+>', re.S)
        self.remove_blank = re.compile(r'^\s+|\s+$', re.S)
        self.remove_special = re.compile(r'&#[\s\S]*?;', re.S)
        self.logger = logging.getLogger("spider.parser")

    # 解析JSON
    def extraction_json(self, all_field_obj, all_field_name=None, current_spider_url=''):
        """
        @all_field_obj：所有层级已抓取字段的key-value dict
        @all_field_name：所有层级已抓取字段的key list
        """
        assert self.rules
        extract_results = []
        # 预先剔除源码的规则集合
        source_code_eliminate_rule_list = []
        # 抽取_content字段用于循环的规则集合
        _content_extract_rule_list = []
        # 抽取其他字段的规则集合
        others_extract_rule_list = []
        # 字段的替换规则集合
        others_replace_rule_list = []

        self.rules = json.loads(self.rules)

        # 规则分类
        for rule_dict in self.rules:
            # 遍历规则列表

            # 字段名
            field = rule_dict["field"]
            # 规则类型
            rule_type = rule_dict["rule_type"]
            # 用于标识当前规则是用于数据提取还是替换
            useful = rule_dict["useful"]

            if field in all_field_name:
                raise Exception(f'数据抽取报错：{field}字段抽取规有误，字段名称不可重复，请检查 - {str(rule_dict)}')

            if not field and not useful:
                # 剔除网页源码
                if rule_type == 'xpath':
                    source_code_eliminate_rule_list.insert(0, rule_dict)
                elif rule_type == 'regEx':
                    source_code_eliminate_rule_list.insert(-1, rule_dict)
            elif field == '_content' and useful:
                # 抽取用于循环的默认字段
                _content_extract_rule_list.append(rule_dict)
            elif field and useful:
                # 抽取其他字段
                others_extract_rule_list.append(rule_dict)
            elif field and not useful:
                # 替换字段
                others_replace_rule_list.append(rule_dict)
            else:
                raise Exception(f'数据抽取报错：{field}字段抽取规则有误，请检查 - {str(rule_dict)}')
        if source_code_eliminate_rule_list:
            for source_code_eliminate_rule_dict in source_code_eliminate_rule_list:
                rule_type1 = source_code_eliminate_rule_dict["rule_type"]
                rule_str1 = source_code_eliminate_rule_dict["rule"]
                replace1 = source_code_eliminate_rule_dict["replace"]
                if rule_type1 == 'regEx':
                    if not replace1:
                        replace1 = ''
                    self.content = re.sub(rule_str1, replace1, self.content)
            self.content_obj = ExtractionJson(self.content)
        loop_records = []
        if _content_extract_rule_list:
            for rule_dict in _content_extract_rule_list:
                loop_records = self.content_obj.get_list(rule_dict['rule'])

        # 抽取字段
        if loop_records:
            if isinstance(loop_records, dict):
                loop_records = [loop_records]

            for loop_record in loop_records:
                record_result = {}
                if not isinstance(loop_record, dict):
                    loop_record = dict(loop_record)
                if others_extract_rule_list:
                    for rule_dict in others_extract_rule_list:
                        field_value = []
                        field = rule_dict["field"]
                        rule_type = rule_dict["rule_type"]
                        rule_str = rule_dict["rule"]
                        # # 判断字段是否需要直接从源码中取值,预留操作，后期可参考html抽取
                        # is_from_source_code = rule_dict.get('is_from_source_code', '')
                        try:
                            if rule_type == 'json_key':
                                field_value = [get_record(rule_str, loop_record)]
                            elif rule_type == 'regEx':
                                field_value = re.search(rule_str, str(loop_record))
                            elif rule_type == 'const':
                                if rule_str == 'yyyy-mm-dd':
                                    rule_str = time.strftime("%Y-%m-%d", time.localtime())
                                    field_value = [rule_str]
                                else:
                                    field_value = [rule_str]
                            elif rule_type == 'reference':
                                merged_dict = {**record_result, **all_field_obj}
                                if re.findall('\=\>(.+?)\<\=', rule_str):
                                    rule_str_temp = rule_str
                                    referenced_field_name_list = re.findall('\=\>(.+?)\<\=',
                                                                            rule_str_temp)
                                    for referenced_field_name in referenced_field_name_list:
                                        if merged_dict.get(referenced_field_name, None) is None:
                                            raise Exception(
                                                f'数据抽取报错：字段{field}抽取出错，请检查-引用其他字段的顺序，被引用字段必须先配置')
                                        rule_str_temp = re.sub(
                                            fr'\=\>{referenced_field_name}\<\=',
                                            merged_dict[referenced_field_name], rule_str_temp)
                                    field_value = [rule_str_temp]
                                else:
                                    if rule_str == '_from_current_layer_spider_url':
                                        field_value = [current_spider_url]
                                    else:
                                        if not merged_dict.get(rule_str, ''):
                                            raise Exception(
                                                f'数据抽取报错：字段{field}抽取出错，请检查-引用其他字段的顺序，被引用字段必须先配置')
                                        field_value = [merged_dict[rule_str]]
                        except Exception as e:
                            raise Exception(f'数据抽取报错：{field}字段抽取出错，请检查 - {str(traceback.format_exc())}')

                        if field_value and field_value[0]:
                            # 去标签/前后空格
                            field_result = self.remove_tag.sub('', str(field_value[0]))
                            field_result = self.remove_special.sub(' ', str(field_result))
                            field_result = field_result.replace('•', '·').replace('&#8226;',
                                                                                  '·').replace(
                                '\u2022',
                                '·').replace(
                                '\u2003', ' ').replace(u'\xa0', ' ').replace(u'\xa0', ' ').replace(
                                '\u0020',
                                ' ')  # NBSP 占位符去除 20221117添加
                            record_result[field] = self.remove_blank.sub('', field_result)
                        else:
                            record_result[field] = ''

                    # 做替换操作
                    if others_replace_rule_list:
                        for replace_field in others_replace_rule_list:
                            if replace_field['field'] != '_content':
                                field_processed = ''
                                rule_type = replace_field["rule_type"]
                                rule_str = replace_field["rule"]
                                replace = replace_field["replace"]
                                if not replace:
                                    replace = ''
                                try:
                                    if rule_type == 'regEx':
                                        field_processed = (lambda x: re.sub(rule_str, replace, x))(
                                            record_result[replace_field['field']])
                                    elif rule_type == 'const':
                                        if record_result.get(replace_field['field'],
                                                             '') == '':  # 为空则用默认值填充
                                            if rule_str == 'yyyy-mm-dd':
                                                rule_str = time.strftime("%Y-%m-%d",
                                                                         time.localtime())
                                            field_processed = rule_str
                                    elif rule_type == 'reference':
                                        merged_dict2 = {**record_result, **all_field_obj}
                                        if re.findall('\=\>(.+?)\<\=', rule_str):
                                            rule_str_temp = rule_str
                                            referenced_field_name_list2 = re.findall(
                                                '\=\>(.+?)\<\=', rule_str_temp)
                                            for referenced_field_name2 in referenced_field_name_list2:
                                                if merged_dict2.get(referenced_field_name2,
                                                                    None) is None:
                                                    raise Exception(
                                                        f'数据抽取报错：字段{replace_field["field"]}抽取出错，请检查-引用其他字段的顺序，被引用字段必须先配置')
                                                rule_str_temp = re.sub(
                                                    fr'\=\>{referenced_field_name2}\<\=',
                                                    merged_dict2[referenced_field_name2],
                                                    rule_str_temp)
                                            rule_str = rule_str_temp
                                        else:
                                            if rule_str == '_from_current_layer_spider_url':
                                                rule_str = current_spider_url
                                            else:
                                                if not merged_dict2.get(rule_str, ''):
                                                    raise Exception(
                                                        f'数据抽取报错：字段{replace_field["field"]}抽取出错，请检查-引用其他字段的顺序，被引用字段必须先配置')
                                                rule_str = merged_dict2[rule_str]
                                        field_processed = rule_str
                                except Exception as e:
                                    raise Exception(
                                        f'数据抽取报错：{replace_field["field"]}剔除字段出错，请检查 - {str(traceback.format_exc())}')
                                else:
                                    record_result[replace_field['field']] = field_processed

                # 过滤记录
                filter_flag = 0
                for rule_dict in others_extract_rule_list:
                    filter_flag = 0
                    # 单个过滤条件
                    field = rule_dict.get("field", None)
                    try:
                        filter_rule = rule_dict.get("filter", None)
                        if filter_rule:
                            if re.match('^\=\>必须为空\<\=$', filter_rule):
                                if record_result.get(f'{field}', ''):
                                    filter_flag = 1
                                    break
                                continue
                            if re.match('^\=\>不能为空\<\=$', filter_rule):
                                if not record_result.get(f'{field}', ''):
                                    filter_flag = 1
                                    break
                                continue
                            filter_rules = r'(' + filter_rule + ')'
                            if not re.search(filter_rules, record_result[field]):
                                self.logger.debug(f'过滤该条记录 - 字段{field} - 不包含{filter_rule}')
                                filter_flag = 1
                                break

                        filter_rule = rule_dict.get("nofilter", None)
                        if filter_rule:
                            filter_rules = r'(' + filter_rule + ')'
                            if re.search(filter_rules, record_result[field]):
                                self.logger.debug(f'过滤该条记录 - 字段{field} - 包含{filter_rule}')
                                filter_flag = 1
                                break
                    except Exception as e:
                        self.logger.debug(f'数据抽取报错：字段{field} -过滤出错！- {str(traceback.format_exc())}')

                if not filter_flag:
                    record_result['record_html'] = str(loop_record)
                    extract_results.append(record_result)
        else:
            raise Exception('数据抽取报错：未获取到_content记录,请检查_content规则!')

        return extract_results
