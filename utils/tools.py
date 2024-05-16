#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   tools.py
@Time    :   2021/01/02 17:45:22
@Author  :   Jianhua Ye
@Phone    :   15655140926
@E-mail :   yejh@finchina.com
@License :   (C)Copyright Financial China Information & Technology Co., Ltd.
@Desc    :   Description
@Version :   1.0
'''

import hashlib
import html
import importlib
# here put the import lib
import math
import os
import random
import re
import socket
import uuid
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from lxml import etree
from utils.reqUtil import OhMyReq
REQ = OhMyReq()


def get_page(url, headers, postdata, charset, pagematch):
    """
    :function: 获取最大页码
    :return: str
    """

    try:
        if isinstance(url, str):
            # get
            try:
                resource = REQ.get_html(url=url, headers=headers, postdata=postdata, charset=charset)
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
            total = re.search(match_list[0].replace(r'(',''), resource['resource'])
            total = re.sub('\D', '', total[0])
            maxpage = math.ceil(int(total) / int(match_list[1].replace(r')', '')))
        else:
            maxpage = re.search(pagematch, resource['resource'])
            maxpage = re.sub('\D', '', maxpage[0])

    finally:
        return maxpage

class OhMyExtractor(object):
    """
    @function: 抽取类
    """

    @staticmethod
    def get_ip():
        """
        :function: 获取本地ip
        :return: str
        """
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(('8.8.8.8', 80))
            ip = s.getsockname()[0]
        except Exception as e:
            print(e)
            return ''
        else:
            return ip
        finally:
            s.close()

    @staticmethod
    def get_urls(url, headers, postdata, pagestyle, pagerule, ishistory,charset):
        """
        :function: 根据配置表生成待抓取网址
        :param : url-网址 postdata-post参数 pagestyle-翻页样式 pagerule-翻页规则 ishistory-是否抓历史
        :memo : 先postdata判断get/post 再判断是否抓历史 然后根据参数生成网址
        :return: list
        """
        assert ishistory in [0, 1, 2, 3]
        url_list = []
        if pagestyle is None:
            pagestyle = ''
        if pagerule is None:
            pagerule = '1, 1, 1'
        pagerule = pagerule.replace(r'，', r',')  # 对中文逗号进行替换,增加容错性

        if not postdata:
            # get
            if ishistory == 0:
                # 不抓历史
                url_list.append(url)
                return url_list

            elif ishistory == 1 or ishistory == 3:
                # 抓全部历史
                if '{0}' in pagestyle:
                    #最大页码支持取源代码，配置规则用“(正则)”代替数字
                    if re.search('\(',pagerule):
                        rule = re.search('(\([\s\S]*?\)),([^,]*?),([^,]*?)$',pagerule)
                        if rule:
                            page_param = [rule.group(1), rule.group(2), rule.group(3)]
                    else:
                        page_param = pagerule.split(',')
                    if re.search('\(',page_param[0]):
                        maxpage = get_page(url, headers, postdata, charset, page_param[0])
                        page_param[0] = maxpage
                    if re.search('\(',page_param[1]):
                        maxpage = get_page(url, headers, postdata, charset, page_param[0])
                        page_param[1] = maxpage
                    page_param = list(map(lambda x: int(x), page_param[:3]))
                    if (page_param[0] > page_param[1]) and page_param[2] < 0:
                        # 倒序
                        for i in range(page_param[0], page_param[1] - 1, page_param[2]):
                            url_list.append(pagestyle.replace(r'{0}', str(i)))
                    else:
                        # 正序
                        for i in range(page_param[0], page_param[1] + 1, page_param[2]):
                            url_list.append(pagestyle.replace(r'{0}', str(i)))
                url_list.insert(0, url)
                return url_list

            elif ishistory == 2:
                # 抓日常，设置为前五页
                if '{0}' in pagestyle:
                    page_param = pagerule.split(',')
                    page_param = list(map(lambda x: int(x), page_param))
                    if (page_param[0] > page_param[1]) and page_param[2] < 0:
                        # 倒序
                        for i in range(page_param[0], page_param[0] + (page_param[2] * 4), page_param[2]):
                            url_list.append(pagestyle.replace(r'{0}', str(i)))
                    else:
                        # 正序
                        for i in range(page_param[0], page_param[0] + (page_param[2] * 4), page_param[2]):
                            url_list.append(pagestyle.replace(r'{0}', str(i)))
                url_list.insert(0, url)
                return url_list

        else:
            # post
            if ishistory == 0:
                # 不抓历史
                url_list.append((url, postdata.replace(r'{0}', '1')))
                return url_list

            elif ishistory == 1:
                # 抓全部历史
                if '{0}' in postdata:
                    page_param = pagerule.split(',')
                    page_param = list(map(lambda x: int(x), page_param))
                    if (page_param[0] > page_param[1]) and page_param[2] < 0:
                        # 倒序
                        for i in range(page_param[0], page_param[1] - 1, page_param[2]):
                            url_list.append((url, postdata.replace(r'{0}', str(i))))
                    else:
                        for i in range(page_param[0], page_param[1] + 1, page_param[2]):
                            url_list.append((url, postdata.replace(r'{0}', str(i))))
                url_list.insert(0, (url, postdata.replace(r'{0}', '1')))
                return url_list

            elif ishistory == 2:
                # 抓日常，设置为前5页
                if '{0}' in postdata:
                    page_param = pagerule.split(',')
                    page_param = list(map(lambda x: int(x), page_param))
                    if (page_param[0] > page_param[1]) and page_param[2] < 0:
                        # 倒序
                        for i in range(page_param[0], page_param[0] + (page_param[2] * 4), page_param[2]):
                            url_list.append((url, postdata.replace(r'{0}', str(i))))
                    else:
                        # 正序
                        for i in range(page_param[0], page_param[0] + (page_param[2] * 4), page_param[2]):
                            url_list.append((url, postdata.replace(r'{0}', str(i))))
                url_list.insert(0, (url, postdata.replace(r'{0}', '1')))
                return url_list

    @staticmethod
    def generate_dup_text(content_dict, dup_rule):
        """
        :function: 根据配置表进行数据去重
        :param : content_dict-待比对内容  dup_rule-去重规则
        :memo : 先根据去重规则去除带比对内容,拼接成字符串,转MD5并生成GUID
        :return: boolean
        """
        assert dup_rule
        dup_rule = dup_rule.replace('，', ',')
        dup_rule_list = dup_rule.split(',')
        dup_text = ''
        # 字段转小写再比对
        content_dict_lower = []
        content_dict_lower = [i.lower() for i in list(content_dict.keys())]
        for dup_field in dup_rule_list:
            if dup_field.lower() in content_dict_lower:
                dup_text += content_dict[dup_field]
            else:
                raise Exception(f'去重字段不在抓取结果中 - {dup_rule} - {content_dict_lower}')
        md5 = OhMyExtractor.hash_encryto(dup_text, method='MD5')
        guid = OhMyExtractor.generate_GUID(md5, traceable=True)
        return str(guid).upper()

    @staticmethod
    def hash_encryto(content, method='MD5'):
        """
        :function: 对字段进行Hash加密
        :param : content-待加密内容 method-加密方法(MD5,SHA1)
        :return: str
        """
        conetent = content.encode("utf8")
        if method.lower() == 'md5':
            hasher = hashlib.md5(conetent)
        elif method.lower() == 'sha1':
            hasher = hashlib.sha1(conetent)
        else:
            hasher = hashlib.md5(conetent)
        return hasher.hexdigest()

    @staticmethod
    def generate_GUID(content='', traceable=True):
        """
        :function: 对字段进行Hash加密
        :param : content-待生成内容 traceable-是否可回溯
        :return: str
        """
        if not content:
            return str(uuid.uuid1())
        else:
            if traceable:
                return str(uuid.uuid3(uuid.NAMESPACE_URL, content))
            else:
                return str(uuid.uuid1())

    @staticmethod
    def complete_url(url_com='', url_uncom=''):
        """
        :function: 对网址进行补全
        :param : url_com-完整url url_uncom-待补全url
        :return: str
        """
        return urljoin(url_com, url_uncom)

    @staticmethod
    def save_html(content, rules, guid, fileRoot, res_type=None):
        """
        :function: 保存源码
        :param : content-网页源码 rules-列表规则 guid-去重规则生成的guid
        :return: str
        """
        typeDict = {'0': 'html', '1': 'json'}
        moduleDict = {1: 'ktgg', 2: 'sdgg', 3: 'laxx', 4: 'cpws', 5: 'fygg'}

        if res_type == 'content':
            # 保存正文页
            fileType = typeDict[(rules['isjson'].split(','))[1]]
            module = moduleDict[rules['modulecode']]
            if not module:
                raise Exception('模块名为空,请检查规则!')

            conpath = os.path.join(fileRoot, 'litigation', 'content', module, str(rules['datasource']),
                                   str(rules['sort']), f'{guid}.{fileType}')
            relpath = 'litigation/content/{}/{}/{}/{}'.format(module, str(rules['datasource']), str(rules['sort']),
                                                              f'{guid}.{fileType}')
            if not os.path.isdir(os.path.dirname(conpath)):
                os.makedirs(os.path.dirname(conpath))
            try:
                with open(conpath, 'w', encoding='UTF-8') as f:
                    f.write(content)
            except Exception as e:
                raise Exception(f'源码保存失败 - {e}')
            else:
                return conpath, relpath

        elif res_type == 'list':
            # 保存列表页
            fileType = typeDict[(rules['isjson'].split(','))[0]]
            module = moduleDict[rules['modulecode']]

            if not module:
                raise Exception('模块名为空,请检查规则!')

            conpath = os.path.join(fileRoot, 'litigation', 'list', module, str(rules['datasource']), str(rules['sort']),
                                   f'{guid}.{fileType}')
            relpath = 'litigation/list/{}/{}/{}/{}'.format(module, str(rules['datasource']), str(rules['sort']),
                                                           f'{guid}.{fileType}')
            if not os.path.isdir(os.path.dirname(conpath)):
                os.makedirs(os.path.dirname(conpath))
            try:
                with open(conpath, 'w', encoding='UTF-8') as f:
                    f.write(content)
            except Exception as e:
                raise Exception(f'源码保存失败 - {e}')
            else:
                return conpath, relpath

    @staticmethod
    def replace_attr(base_url, content):
        """
        :function: 保存源码
        :param :  base_url-url content-网页源码
        :return: str
        """
        is_img = 0
        is_file = 0
        content_dom = etree.HTML(content)
        href_list = content_dom.xpath('//a/@href')
        embed_list = content_dom.xpath('//embed/@src')
        href_list.extend(embed_list)
        img_list = content_dom.xpath('//img/@src')
        if img_list:
            for img in img_list[::-1]:
                if str(img).endswith('gif'):
                    img_list.remove(img)
            content_notag = re.sub(r'<(style|script)[\s\S]*?</(style|script)>', '', content, flags=re.IGNORECASE)
            content_notag = re.sub(r'<!--[\s\S]*?-->', '', content_notag)
            content_notag = re.sub(r'<[^>]*?>', '', content_notag)
            content_notag = re.sub(r'\s', '', content_notag)
            if len(content_notag) < 2:
                is_img = 1

        if href_list:
            for href in href_list[::-1]:
                if not (href == '' or href == '#' or ('javascript' in href and '/' not in href)):
                    is_file = 1
                else:
                    href_list.remove(href)

        for url in list(set(img_list).union(href_list)):
            url_com = OhMyExtractor.complete_url(base_url, url)
            url_com = html.unescape(url_com)
            content = content.replace(url, url_com)

        return content, is_img, is_file

    @staticmethod
    def replace_files(base_url, content):
        """
        :function: 保存源码
        :param :  base_url-url content-网页源码
        :return: str
        :memo : 弃用
        """
        is_file = 0
        content_lower = content.lower()
        if '<a' in content_lower:
            soup = BeautifulSoup(content, 'lxml')
            file_list = soup.select('a')
            if file_list:
                for file_tag in file_list:
                    if 'href' not in str(file_tag).lower():
                        continue

                    url = file_tag["href"]
                    if url.startswith('javas') or url.startswith('void') or url.startswith('#') or url.endswith(
                            '.html') or url.endswith('.htm'):
                        continue

                    is_file = 1
                    if not re.match('http', url, flags=re.IGNORECASE):
                        url_com = OhMyExtractor.complete_url(base_url, url)
                        content = (lambda x: re.sub(url, url_com, x))(content)

        return content, is_file

    @staticmethod
    def generate_uniqueGuid(url, dup_rule):
        """
        :function: 根据配置表进行数据去重
        :param : content_dict-待比对内容  dup_rule-去重规则
        :memo : 先根据去重规则去除带比对内容,拼接成字符串,转MD5并生成GUID
        :return: str
        """
        assert dup_rule
        dup_rule = dup_rule.replace('，', ',')
        dup_rule_list = dup_rule.split(',')
        if '_url' in dup_rule_list and len(dup_rule_list) == 1:
            # 去重字段仅有url
            return OhMyExtractor.hashsecnew(url)
        else:
            return OhMyExtractor.hashsecnew(url) + '-' + OhMyExtractor.sixrandom()

    @staticmethod
    def sixrandom():
        codeStr = "qwertyuiopasdfghjklzxcvbnm0123456789"
        codelist = list(codeStr)
        random.shuffle(codelist)
        resultlist = []
        for i in range(6):
            while True:
                s = random.choice(codelist)
                if s not in resultlist:
                    resultlist.append(s)
                    break
        random.shuffle(resultlist)
        return "".join(resultlist)

    @staticmethod
    def hashsecnew(url):
        url = str.lower(url)
        m = hashlib.md5()
        m.update(url.encode('utf-8'))
        s1 = re.search('://([^/]*)', url)
        s2 = re.sub(':\d{1,}$', '', s1[1])
        m1 = hashlib.sha1()
        m1.update(s2.encode('utf-8'))
        s3 = m1.hexdigest()
        s4 = re.sub('.{30}$', '', s3)
        hash1 = hashlib.md5(s3.encode('utf-8'))
        hash1.update(url.encode('utf-8'))
        return (str(s4) + '-' + hash1.hexdigest())

    @staticmethod
    def exec_plugins(content, plugin):
        """
        :function: 根据配置表进行数据去重
        :param : content-待处理内容  plugin-插件规则
        :memo : 先根据去重规则去除带比对内容,拼接成字符串,转MD5并生成GUID
        :return: str
        """
        # plugin = {"inputKey": "publishdate,content", "pluginName": "plugins", "outputKey": "publishdate", "memo": "\u8f6c\u6362\u6d4b\u8bd5"}

        inputKey = plugin['inputKey']
        pluginName = plugin['pluginName']
        outputKey = plugin['outputKey']
        try:
            plugin_module = importlib.import_module(pluginName)
            importlib.reload(plugin_module)
            plugin = plugin_module.Exec_Plugins(content)
        except Exception as e:
            raise Exception(f'{pluginName} - 插件初始化失败 - {e}')

        else:
            try:
                content = plugin.exec_plugin()
            except Exception as e:
                raise Exception(f'{pluginName} - 插件处理失败 - {e}')
            else:
                return content
