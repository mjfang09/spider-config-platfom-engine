#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Project ：  spider-nav-config-platform 
@File    :   spider_constant.py
@Time    :   2024/2/20 13:39
@Author  :   MengJia_Fang 501738
@E-mail  :   fangmj@finchina.com
@License :   (C)Copyright Financial China Information & Technology Co., Ltd.
@Desc    :   统一定义常量类
@Version :   1.0
"""
# 不使用配置框架抓取
SPIDER_NOT_USE_RULES = 0
# 使用配置框架抓取
SPIDER_USE_RULES = 1

# 不使用代理
SPIDER_NOT_USE_PROXY = 0
# 使用公司代理
SPIDER_USE_COMPANY_PROXY = 1
# 使用付费代理
SPIDER_USE_CHARGE_PROXY = 2

# 抓取配置任务已升级
SPIDER_CONFIG_UPGRADED = 1
# 抓取配置任务未升级
SPIDER_CONFIG_NOT_UPGRADE = 0

# 大于2就是多级，小于等于2是非多级
SPIDER_LAYER_2 = 2

# 正常，消息不设置优先级
RABBIT_MQ_MSG_PRIORITY_0 = 0
# 优先级一般
RABBIT_MQ_MSG_PRIORITY_2 = 2
# 优先级较高
RABBIT_MQ_MSG_PRIORITY_5 = 5
# 优先级很高
RABBIT_MQ_MSG_PRIORITY_9 = 9

COOKIE_BASE_DIR = r'\\fileserver.finchina.local\hct\净值配置框架\cookie'
URL_FROM_FILE_BASE_DIR = r'\\fileserver.finchina.local\hct\净值配置框架\url_file'

# mq
# 升级后,调度程序会发消息到这个队列
TEST_LC_JZ_CRON_TASK_QUEUE_NAME = 'test_lc_jz_cron_task'
LC_JZ_CRON_TASK_QUEUE_NAME = 'lc_jz_cron_task'

TEST_LC_JZ_DATA_TO_BE_DOWNLOADED_QUEUE_NAME = 'test_lc_jz_data_to_be_downloaded'
LC_JZ_DATA_TO_BE_DOWNLOADED_QUEUE_NAME = 'lc_jz_data_to_be_downloaded'

TEST_LC_JZ_DATA_TO_BE_EXTRACT_QUEUE_NAME = 'test_lc_jz_data_to_be_extract'
LC_JZ_DATA_TO_BE_EXTRACT_QUEUE_NAME = 'lc_jz_data_to_be_extract'

TEST_LC_JZ_DATA_TO_BE_DEDUPLICATED_QUEUE_NAME = 'test_lc_jz_data_to_be_deduplicated'
LC_JZ_DATA_TO_BE_DEDUPLICATED_QUEUE_NAME = 'lc_jz_data_to_be_deduplicated'

TEST_LC_JZ_DATA_TO_BE_STORED_QUEUE_NAME = 'test_lc_jz_data_to_be_stored'
LC_JZ_DATA_TO_BE_STORED_QUEUE_NAME = 'lc_jz_data_to_be_stored'

TEST_LC_JZ_File_TO_BE_EXTRACT_QUEUE_NAME = 'test_lc_jz_file_to_be_extract'
LC_JZ_File_TO_BE_EXTRACT_QUEUE_NAME = 'lc_jz_file_to_be_extract'

# 死信队列名称
DEAD_TEST_LC_JZ_CRON_TASK_QUEUE_NAME = 'dead_test_lc_jz_cron_task'
DEAD_LC_JZ_CRON_TASK_QUEUE_NAME = 'dead_lc_jz_cron_task'

DEAD_TEST_LC_JZ_DATA_TO_BE_DOWNLOADED_QUEUE_NAME = 'dead_test_lc_jz_data_to_be_downloaded'
DEAD_LC_JZ_DATA_TO_BE_DOWNLOADED_QUEUE_NAME = 'dead_lc_jz_data_to_be_downloaded'

DEAD_TEST_LC_JZ_DATA_TO_BE_EXTRACT_QUEUE_NAME = 'dead_test_lc_jz_data_to_be_extract'
DEAD_LC_JZ_DATA_TO_BE_EXTRACT_QUEUE_NAME = 'dead_lc_jz_data_to_be_extract'

DEAD_TEST_LC_JZ_DATA_TO_BE_DEDUPLICATED_QUEUE_NAME = 'dead_test_lc_jz_data_to_be_deduplicated'
DEAD_LC_JZ_DATA_TO_BE_DEDUPLICATED_QUEUE_NAME = 'dead_lc_jz_data_to_be_deduplicated'

DEAD_TEST_LC_JZ_DATA_TO_BE_STORED_QUEUE_NAME = 'dead_test_lc_jz_data_to_be_stored'
DEAD_LC_JZ_DATA_TO_BE_STORED_QUEUE_NAME = 'dead_lc_jz_data_to_be_stored'

DEAD_TEST_LC_JZ_File_TO_BE_EXTRACT_QUEUE_NAME = 'dead_test_lc_jz_file_to_be_extract'
DEAD_LC_JZ_File_TO_BE_EXTRACT_QUEUE_NAME = 'dead_lc_jz_file_to_be_extract'

# MONGO_DATA_FLAG = "zzb"
FIRST_LAYER_URL_GENERATE_FLAG = "first_layer_url_generate_program"
DATA_DOWNLOAD_FLAG = "data_download_program"
DATA_EXTRACT_FLAG = "data_extract_program"
DATA_DEDUPLICATE_FLAG = "data_deduplicate_program"
DATA_STORE_FLAG = "data_store_program"
FILE_EXTRACT_FLAG = "file_extract_program"
DIRECT_RUN_FORMAL_SPIDER_FLAG = "direct_run_formal_spider_program"

# 死信队列处理消息程序flag
DEAD_DATA_DOWNLOAD_FLAG = "dead_data_download_program"
DEAD_DATA_EXTRACT_FLAG = "dead_data_extract_program"
DEAD_DATA_DEDUPLICATE_FLAG = "dead_data_deduplicate_program"
DEAD_DATA_STORE_FLAG = "dead_data_store_program"
DEAD_FILE_EXTRACT_FLAG = "dead_file_extract_program"
DEAD_CRON_TASK_FLAG = "dead_cron_task_program"

# 留痕目录
SAVE_SOURCE_CODE_FILE_PATH = r'\\fileserver.finchina.local\hct\理财净值溯源文件\净值采集框架\测试'

# 公告附件下载目录
SAVE_ATTACHMENT_DOWNLOAD_PATH = r'\\fileserver.finchina.local\hct\spider-nav-config-platform2\attachment_download'

# 过滤死信队列信源处理配置文件
FILTER_DEAD_MSG_CONFIG_FILE_PATH = r'\\fileserver.finchina.local\hct\spider-nav-config-platform2\filter_dead_msg_config\filter_dead_msg_config.toml'

# 用于自动抽取附件
SUFFIX_DICT = {
    "application/x-001": ".001",
    "application/x-301": ".301",
    "text/h323": ".323",
    "application/x-906": ".906",
    "drawing/907": ".907",
    "application/x-a11": ".a11",
    "audio/x-mei-aac": ".acp",
    "application/postscript": ".ai",
    "application/postscript": ".eps",
    "application/postscript": ".ps",
    "audio/aiff": ".aiff",
    "application/x-anv": ".anv",
    "text/asa": ".asa",
    "video/x-ms-asf": ".asf",
    "video/x-ms-asf": ".asx",
    "text/asp": ".asp",
    "audio/basic": ".au",
    "audio/basic": ".snd",
    "video/avi": ".avi",
    "application/vnd.adobe.workflow": ".awf",
    "application/x-bmp": ".bmp",
    "application/x-bot": ".bot",
    "application/x-c4t": ".c4t",
    "application/x-c90": ".c90",
    "application/x-cals": ".cal",
    "application/vnd.ms-pki.seccat": ".cat",
    "application/x-netcdf": ".cdf",
    "application/x-cdr": ".cdr",
    "application/x-cel": ".cel",
    "application/x-x509-ca-cert": ".cer",
    "application/x-x509-ca-cert": ".crt",
    "application/x-x509-ca-cert": ".der",
    "application/x-g4": ".cg4",
    "application/x-g4": ".g4",
    "application/x-g4": ".ig4",
    "application/x-cgm": ".cgm",
    "application/x-cit": ".cit",
    "application/x-cmp": ".cmp",
    "application/x-cmx": ".cmx",
    "application/x-cot": ".cot",
    "application/pkix-crl": ".crl",
    "application/x-csi": ".csi",
    "text/css": ".css",
    "application/x-cut": ".cut",
    "application/x-dbf": ".dbf",
    "application/x-dbm": ".dbm",
    "application/x-dbx": ".dbx",
    "application/x-dcx": ".dcx",
    "application/x-dgn": ".dgn",
    "application/x-dib": ".dib",
    "application/msword": ".doc",
    "application/x-drw": ".drw",
    "Model/vnd.dwf": ".dwf",
    "application/x-dwf": ".dwf",
    "application/x-dwg": ".dwg",
    "application/x-dxb": ".dxb",
    "application/x-dxf": ".dxf",
    "application/vnd.adobe.edn": ".edn",
    "application/x-emf": ".emf",
    "message/rfc822": ".eml",
    "message/rfc822": ".nws",
    "application/x-epi": ".epi",
    "application/x-ps": ".eps",
    "application/x-ps": ".ps",
    "application/x-ebx": ".etd",
    "image/fax": ".fax",
    "application/vnd.fdf": ".fdf",
    "application/fractals": ".fif",
    "application/x-frm": ".frm",
    "application/x-gbr": ".gbr",
    "image/gif": ".gif",
    "application/x-gl2": ".gl2",
    "application/x-gp4": ".gp4",
    "application/x-hgl": ".hgl",
    "application/x-hmr": ".hmr",
    "application/x-hpgl": ".hpg",
    "application/x-hpl": ".hpl",
    "application/mac-binhex40": ".hqx",
    "application/x-hrf": ".hrf",
    "application/hta": ".hta",
    "text/x-component": ".htc",
    "text/webviewhtml": ".htt",
    "application/x-icb": ".icb",
    "image/x-icon": ".ico",
    "application/x-ico": ".ico",
    "application/x-iff": ".iff",

    "application/x-igs": ".igs",
    "application/x-iphone": ".iii",
    "application/x-img": ".img",
    "image/jpg": ".img",
    "application/x-internet-signup": ".ins",
    "application/x-internet-signup": ".isp",
    "video/x-ivf": ".IVF",
    "java/*": ".java",
    "application/x-jpe": ".jpe",
    "image/jpeg": ".jpeg",
    "application/x-jpg": ".jpg",
    "application/x-javascript": ".js",
    "application/x-javascript": ".ls",
    "application/x-javascript": ".mocha",
    "audio/x-liquid-file": ".la1",
    "application/x-laplayer-reg": ".lar",
    "application/x-latex": ".latex",
    "audio/x-liquid-secure": ".lavs",
    "application/x-lbm": ".lbm",
    "audio/x-la-lms": ".lmsff",
    "application/x-ltr": ".ltr",
    "video/x-mpeg": ".m1v",
    "video/x-mpeg": ".m2v",
    "video/x-mpeg": ".mpe",
    "video/x-mpeg": ".mps",
    "audio/mpegurl": ".m3u",
    "video/mpeg4": ".m4e",
    "video/mpeg4": ".mp4",
    "video/mpeg": ".mp2v",
    "video/mpeg": ".mpv2",
    "application/x-mac": ".mac",
    "application/x-troff-man": ".man",
    "application/msaccess": ".mdb",
    "application/x-mdb": ".mdb",
    "application/x-shockwave-flash": ".mfp",
    "application/x-shockwave-flash": ".swf",
    "application/x-mi": ".mi",
    "audio/mid": ".mid",
    "application/x-mil": ".mil",
    "audio/x-musicnet-download": ".mnd",
    "audio/x-musicnet-stream": ".mns",

    "video/x-sgi-movie": ".movie",
    "audio/mp1": ".mp1",
    "audio/mp2": ".mp2",

    "audio/mp3": ".mp3",

    "video/x-mpg": ".mpa",
    "application/vnd.ms-project": ".mpd",
    "application/vnd.ms-project": ".mpp",
    "application/vnd.ms-project": ".mpt",
    "application/vnd.ms-project": ".mpw",
    "application/vnd.ms-project": ".mpx",
    "video/mpg": ".mpeg",
    "video/mpg": ".mpg",
    "video/mpg": ".mpv",
    "audio/rn-mpeg": ".mpga",
    "application/x-mmxp": ".mxp",
    "image/pnetvue": ".net",
    "application/x-nrf": ".nrf",

    "text/x-ms-odc": ".odc",
    "application/x-out": ".out",
    "application/pkcs10": ".p10",
    "application/x-pkcs12": ".p12",
    "application/x-pkcs12": ".pfx",
    "application/x-pkcs7-certificates": ".p7b",
    "application/x-pkcs7-certificates": ".spc",
    "application/pkcs7-mime": ".p7c",
    "application/pkcs7-mime": ".p7m",
    "application/x-pkcs7-certreqresp": ".p7r",
    "application/pkcs7-signature": ".p7s",
    "application/x-pc5": ".pc5",
    "application/x-pci": ".pci",
    "application/x-pcl": ".pcl",
    "application/x-pcx": ".pcx",
    "application/pdf": ".pdf",
    "application/vnd.adobe.pdx": ".pdx",

    "application/x-pgl": ".pgl",
    "application/x-pic": ".pic",
    "application/vnd.ms-pki.pko": ".pko",
    "application/x-perl": ".pl",
    "audio/scpls": ".pls",
    "audio/scpls": ".xpl",
    "application/x-plt": ".plt",
    "image/png": ".png",
    "application/x-png": ".png",
    "application/vnd.ms-powerpoint": ".pot",
    "application/vnd.ms-powerpoint": ".ppa",
    "application/vnd.ms-powerpoint": ".pps",
    "application/vnd.ms-powerpoint": ".ppt",
    "application/vnd.ms-powerpoint": ".pwz",
    "application/x-ppm": ".ppm",
    "application/x-ppt": ".ppt",
    "application/x-pr": ".pr",
    "application/pics-rules": ".prf",
    "application/x-prn": ".prn",
    "application/x-prt": ".prt",
    "application/x-ptn": ".ptn",
    "text/vnd.rn-realtext3d": ".r3t",
    "audio/vnd.rn-realaudio": ".ra",
    "audio/x-pn-realaudio": ".ram",
    "audio/x-pn-realaudio": ".rmm",
    "application/x-ras": ".ras",
    "application/rat-file": ".rat",
    "application/vnd.rn-recording": ".rec",
    "application/x-red": ".red",
    "application/x-rgb": ".rgb",
    "application/vnd.rn-realsystem-rjs": ".rjs",
    "application/vnd.rn-realsystem-rjt": ".rjt",
    "application/x-rlc": ".rlc",
    "application/x-rle": ".rle",
    "application/vnd.rn-realmedia": ".rm",
    "application/vnd.adobe.rmf": ".rmf",
    "application/vnd.rn-realsystem-rmj": ".rmj",

    "application/vnd.rn-rn_music_package": ".rmp",
    "application/vnd.rn-realmedia-secure": ".rms",
    "application/vnd.rn-realmedia-vbr": ".rmvb",
    "application/vnd.rn-realsystem-rmx": ".rmx",
    "application/vnd.rn-realplayer": ".rnx",
    "image/vnd.rn-realpix": ".rp",
    "audio/x-pn-realaudio-plugin": ".rpm",
    "application/vnd.rn-rsml": ".rsml",
    "text/vnd.rn-realtext": ".rt",
    "application/x-rtf": ".rtf",
    "video/vnd.rn-realvideo": ".rv",
    "application/x-sam": ".sam",
    "application/x-sat": ".sat",
    "application/sdp": ".sdp",
    "application/x-sdw": ".sdw",
    "application/x-stuffit": ".sit",
    "application/x-slb": ".slb",
    "application/x-sld": ".sld",
    "drawing/x-slk": ".slk",
    "application/smil": ".smi",
    "application/smil": ".smil",
    "application/x-smk": ".smk",
    "text/plain": ".sol",
    "text/plain": ".sor",
    "text/plain": ".txt",
    "application/futuresplash": ".spl",
    "application/streamingmedia": ".ssm",
    "application/vnd.ms-pki.certstore": ".sst",
    "application/vnd.ms-pki.stl": ".stl",
    "application/x-sty": ".sty",
    "application/x-tdf": ".tdf",
    "application/x-tg4": ".tg4",
    "application/x-tga": ".tga",
    "application/x-tif": ".tif",
    "image/tiff": ".tiff",
    "drawing/x-top": ".top",
    "application/x-bittorrent": ".torrent",
    "application/x-icq": ".uin",
    "text/iuls": ".uls",
    "text/x-vcard": ".vcf",
    "application/x-vda": ".vda",
    "application/vnd.visio": ".vdx",
    "application/vnd.visio": ".vsd",
    "application/vnd.visio": ".vss",
    "application/vnd.visio": ".vst",
    "application/vnd.visio": ".vsw",
    "application/vnd.visio": ".vsx",
    "application/vnd.visio": ".vtx",
    "application/x-vpeg005": ".vpg",
    "application/x-vsd": ".vsd",
    "application/x-vst": ".vst",
    "audio/wav": ".wav",
    "audio/x-ms-wax": ".wax",
    "application/x-wb1": ".wb1",
    "application/x-wb2": ".wb2",
    "application/x-wb3": ".wb3",
    "image/vnd.wap.wbmp": ".wbmp",
    "application/x-wk3": ".wk3",
    "application/x-wk4": ".wk4",
    "application/x-wkq": ".wkq",
    "application/x-wks": ".wks",
    "video/x-ms-wm": ".wm",
    "audio/x-ms-wma": ".wma",
    "application/x-ms-wmd": ".wmd",
    "application/x-wmf": ".wmf",
    "text/vnd.wap.wml": ".wml",
    "video/x-ms-wmv": ".wmv",
    "video/x-ms-wmx": ".wmx",
    "application/x-ms-wmz": ".wmz",
    "application/x-wp6": ".wp6",
    "application/x-wpd": ".wpd",
    "application/x-wpg": ".wpg",
    "application/vnd.ms-wpl": ".wpl",
    "application/x-wq1": ".wq1",
    "application/x-wr1": ".wr1",
    "application/x-wri": ".wri",
    "application/x-wrk": ".wrk",
    "application/x-ws": ".ws",
    "application/x-ws": ".ws2",
    "text/scriptlet": ".wsc",
    "video/x-ms-wvx": ".wvx",
    "application/vnd.adobe.xdp": ".xdp",
    "application/vnd.adobe.xfd": ".xfd",
    "application/vnd.adobe.xfdf": ".xfdf",
    "application/vnd.ms-excel": ".xls",
    "application/x-xls": ".xls",
    "application/x-xlw": ".xlw",
    "application/x-xwd": ".xwd",
    "application/x-x_b": ".x_b",
    "application/vnd.symbian.install": ".sis",
    "application/vnd.symbian.install": ".sisx",
    "application/x-x_t": ".x_t",
    "application/vnd.iphone": ".ipa",
    "application/vnd.android.package-archive": ".apk",
    "application/x-silverlight-app": ".xap",
}
