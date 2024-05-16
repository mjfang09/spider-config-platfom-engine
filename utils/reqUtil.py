#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   reqUtil.py
@Time    :   2020/12/22 09:26:24
@Author  :   Jianhua Ye
@Phone    :   15655140926
@E-mail :   yejh@finchina.com
@License :   (C)Copyright Financial China Information & Technology Co., Ltd.
@Desc    :   请求类
@Version :   1.0
'''

import logging
import random
# here put the import lib
import re
import time
import traceback

import chardet
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


class OhMyReq(object):
    """
    @function: 请求类
    """

    def __init__(self, *args, **kwargs):
        # 源码保存路径
        self.pathfile = 'file'
        # # 代理池
        # self.proxies = ['10.17.205.91:808', '10.17.205.96:808', '']
        # 请求超时时间
        self.timeout = 10
        # 是否SSL验证
        self.verify = False
        # 是否重定向
        self.allow_redirects = True
        # 获取log句柄
        self.logger = logging.getLogger("spider.reqUtil")
        self.current_proxy = {}

    def user_agent(self):
        """
        :function: 随机取user_agent
        :return: str
        """
        user_agent_list = [
            'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/45.0.2454.85 Safari/537.36 115Browser/6.0.3',
            'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50',
            'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50',
            'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0)',
            'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0)',
            'Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1',
            'Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11',
            'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SE 2.X MetaSr 1.0; SE 2.X MetaSr 1.0; .NET CLR 2.0.50727; SE 2.X MetaSr 1.0)',
            'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0',
            'Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1',
        ]
        user_agent = random.choice(user_agent_list)
        return user_agent

    def html_Encoding(self, response, charset=None):
        """
        :function: 处理网页编码
        :param : response-网页响应
        :return: str
        """
        # TODO: 根据库中编码进行解码
        if charset:
            response.encoding = charset
            return response.text
        else:
            content = response.content  # 返回网页二进制源码
            aa = chardet.detect(content)['encoding']  # 返回网页编码
            if aa:
                char = re.search('(gb|GB)', aa)
            else:
                char = ''
                aa = ''
            if char:  # 转码成字符串
                try:
                    content = content.decode('GBK')
                except Exception:
                    content = response.text
            else:
                try:
                    content = content.decode('utf-8')
                except Exception:
                    content = response.text
            return content

    def header_to_dict(self, raw_headers):
        """
        :function: 网页原生请求头转python字典
        :param : response-网页响应
        :return: str
        """
        return dict([line.replace(': ', ':').split(":", 1) for line in raw_headers.split("\n")])

    def get_html(self, url, postdata='', charset='', proxies=[], headers='', env=''):

        """
        :function: 请求源码
        :param : url-网址 headers-头信息 postdata-post参数
        :return: object
        """
        # _http = re.match('http(s)?', url).group()
        flag = 1

        shttp = re.match('(http(s)?)', url).group(1)
        if 1 == 1:
            # if not list_proxies:
            #     return {
            #         'url': f'{url}',
            #         'postdata': f'{postdata}',
            #         'res_code': 'ERROR',
            #         'resource': 'ERROR',
            #     }
            # proxy = list_proxies.pop(random.randint(0, len(list_proxies) - 1))
            if proxies:
                proxy = random.choice(proxies)
            else:
                proxy = None

            if proxy:
                proxies = {shttp: proxy}
            else:
                proxies = {shttp: ''}
            self.current_proxy = proxies
            print("current proxy ", proxies)

            # TODO: 完善通用请求头
            if not headers:
                headers = {
                    "User-Agent": self.user_agent(),
                }
            else:
                if isinstance(headers, dict):
                    pass
                elif not (str(headers).strip().startswith('{')
                          and str(headers).strip().endswith('}')):
                    # 网页原生请求头
                    try:
                        headers = self.header_to_dict(headers)
                    except Exception as e:
                        raise Exception(f'网页请求头转换为字典失败：{traceback.format_exc()}')
                        # headers = {
                        #     "User-Agent": self.user_agent(),
                        # }
                else:
                    headers = dict(headers)

            try:
                if not postdata:  # get
                    if proxies != {"http": "", "https": ""}:
                        response = requests.get(
                            url=url,
                            headers=headers,
                            proxies=proxies,
                            timeout=self.timeout,
                            verify=self.verify,
                            allow_redirects=self.allow_redirects)
                    else:
                        response = requests.get(
                            url=url,
                            headers=headers,
                            timeout=self.timeout,
                            verify=self.verify,
                            allow_redirects=self.allow_redirects)
                else:  # post
                    if proxies != {"http": "", "https": ""}:
                        response = requests.post(
                            url=url,
                            data=postdata,
                            headers=headers,
                            proxies=proxies,
                            timeout=self.timeout,
                            verify=self.verify,
                            allow_redirects=self.allow_redirects)
                    else:
                        response = requests.post(
                            url=url,
                            data=postdata,
                            headers=headers,
                            timeout=self.timeout,
                            verify=self.verify,
                            allow_redirects=self.allow_redirects)

            except Exception as e:
                if not postdata:
                    self.logger.debug(f'GET - 请求出错 - {url} - {proxies} - {e}')
                else:
                    self.logger.debug(f'POST - 请求出错 - {url} - {postdata} - {proxies} - {e}')

                return {
                    'url': f'{url}',
                    'postdata': f'{postdata}',
                    'res_code': '000',
                    'head': '',
                    'resource': f'{e}'
                }

            else:
                flag = 0
                res_code = response.status_code
                head = response.headers
                res = self.html_Encoding(response, charset)

                if env == 'formal':
                    time.sleep(2)

                # TODO: 增加返回码为3XX，允许重定向重复请求

                return {
                    'url': f'{url}',
                    'postdata': f'{postdata}',
                    'res_code': f'{res_code}',
                    'head': f'{head}',
                    'resource': f'{res}'
                }


def main():
    REQ = OhMyReq()
    req = REQ.get_html(
        'https://fids.qrcb.com.cn/20210106/anocement/findAnoceByType?filter=22&pages=1&p=999'
    )
    print(req)


if __name__ == "__main__":
    main()
