#!/usr/bin/env python
# -*- coding:utf-8 -*-

import re

import requests

requests.packages.urllib3.disable_warnings()


def text_replace(content_html):
    content_html = re.sub(u'<(script|SCRIPT)[\s\S]*?</(script|SCRIPT)>', r'', content_html)
    content_html = re.sub('<(style|STYLE)[\s\S]*?</(style|STYLE)>', r'', content_html)
    content_html = re.sub('<!-- S 导航-->[\s\S]*?<!-- E 导航-->', r'', content_html)
    content_html = re.sub('<input[^<>]*?>', r'', content_html)
    content_html = re.sub('&#13;', r'', content_html)
    content_html = re.sub('<a href=\"mailto:[\s\S]*?>', r'', content_html)
    content_html = re.sub('<div id="meta_content"[\s\S]*?<div class="rich_media_content[^>]*?>', r'', content_html)
    content_html = re.sub(u'<div class=\"bdshare_t bds_tools get-codes-bdshare bdsharebuttonbox\"[\s\S]*?</div>', r'',
                          content_html)
    content_html = re.sub(u'<textarea[\s\S]*?</textarea>', r'', content_html)
    content_html = re.sub(u'网友评论\s*?<[^<>]*?>', r'', content_html)
    content_html = re.sub(u'<!--分享按钮-->[\s\S]*?<!--分享按钮 end-->', r'', content_html)  # 11.4
    content_html = re.sub('<!--[\s\S]*?-->', r'', content_html)
    content_html = re.sub('<img[^>]*?分享到(QQ空间|新浪|微博|微信)[^>]*?>', r'', content_html)  # 11.4
    content_html = re.sub('<div[^>]*?url[^>]*?>', r'<div>', content_html)  # 11.4
    content_html = re.sub('<div[^>]*?>[^>]*?<[^>]*?>分享(到)?：[\s\S]*?</div>', r'', content_html)  # 11.4
    content_html = re.sub('(<img)[^>]*?data-echo=([^>]*?)src=[^>]*?>', r'\1 src=\2>', content_html)

    content_html = re.sub(u'data-src=', r'src=', content_html)
    content_html = re.sub(u'<div[^<>]*?>推荐阅读[\s\S]*', r'', content_html)

    content_html = re.sub(u'<div[^<>]*?class=\"navbar navbar-default\"[\s\S]*?</div>', r'', content_html)

    content_html = re.sub('(【上一篇：】|上一篇)[\s\S]*$', r'', content_html)  # 有些特殊报纸的在上面，可能会导致有些网址抓不到内容

    content_html = re.sub(u'^[\s\S]*?<a[^>]*?>首页</a>', r'', content_html)

    content_html = re.sub('(<div[^>]*?id="js_content") style="visibility: hidden;">', r'\1>', content_html)
    content_html = re.sub(u'^[\s\S]*?<[^<>]*?>\s*?(您(当前)?(所在|现在)?的位置|当前位置)(：)?', r'', content_html)

    content_html = re.sub('当前(所在)?(的)?位置：[^>]*?(正文)?[^>]*?<[^>]*?>', r'', content_html)
    content_html = re.sub('\s{50,}', r'', content_html)
    content_html = re.sub('版权所有：[\s\S]*', r'', content_html)
    content_html = re.sub('<li></li>', r'', content_html)
    content_html = re.sub(u'<img[^>]*?alt="到百度[^>]*?>', r'', content_html)
    content_html = re.sub(u'<s class=\"pull-left\"/>', r'', content_html)

    content_html = re.sub(u'<(link|LINK)[^>]*?>', r'', content_html)  # 调用外部样式导致抓取的内容显示不出来
    content_html = re.sub(u'style=\"overflow-x: hidden; word-break: break-all\"', r'', content_html)  # 表格显示不完整，去除
    content_html = re.sub(u'<[^>]*?>(相关(文章|新闻)[^>]*?|为您推荐|相关网站|相关链接：)<[^>]*?>[\s\S]*$', r'',
                          content_html)
    content_html = re.sub(u'<iframe[^>]*?>', r'', content_html)  # 内容显示不出来，去除
    content_html = re.sub(u'style=\"display: none[^>]*?\"', r'', content_html)  # 内容显示不出来，去除

    content_html = re.sub(u'width:100px;margin-left:\d+%;', r'', content_html)  # 内容显示宽度不够

    content_html = re.sub(u'<[^>]*?>(QQ空间|QQ好友|新浪微博)<[^>]*?>', r'', content_html)  # 11.5
    content_html = re.sub(u'视力保护色：', r'', content_html)  # 11.5
    content_html = re.sub(u'<title/>', r'', content_html)  # 11.9内容显示不出来，去除
    content_html = re.sub(u'<s\s[^>]*?>', r'', content_html)  # 11.9文字上有删除线
    content_html = re.sub(u'<ins\s[^>]*?>', r'', content_html)  # 11.9文字下划线
    content_html = re.sub(u'<button[^>]*?>[\s\S]*?</button>\s*?<ul[^>]*?>[\s\S]*?</ul>', r'', content_html)  # 11.9
    content_html = re.sub(r'<button[\s\S]*?</button>', r'', content_html)
    content_html = re.sub(r'&amp;nbsp', r'', content_html)
    content_html = re.sub(r'<del[^>]*?>', r'', content_html)
    content_html = re.sub(r'<a href="#"[^>]*?>\s*?</a>', r'', content_html)
    content_html = re.sub(r'src=\\"', r'src="', content_html)
    content_html = re.sub(r'\\"', r'"', content_html)
    content_html = re.sub(r'(\r|\n)', r'', content_html)
    content_html = re.sub(r'\\/', r'/', content_html)
    content_html = re.sub(r'&ldquo;', r'“', content_html)
    content_html = re.sub(r'&rdquo;', r'”', content_html)
    content_html = re.sub(r'&middot;', r'·', content_html)
    content_html = re.sub(r'\u0028', r'(', content_html)
    content_html = re.sub(r'\u0029', r')', content_html)

    return (content_html)
