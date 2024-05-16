#!/usr/bin/env python
# -*- encoding: utf-8 -*-
"""
@Project ：  spider-nav-config-platform
@File    :   spider_api.py
@Time    :   2023/11/21 17:40
@Author  :   MengJia_Fang 501738
@E-mail  :   fangmj@finchina.com
@License :   (C)Copyright Financial China Information & Technology Co., Ltd.
@Desc    :   Description
@Version :   1.0
"""
import os
import sys

parent_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(parent_path)
from utils.tools import OhMyExtractor
from typing import Optional
from fastapi import FastAPI, BackgroundTasks
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse
from spider.data_download_threads import data_download
from spider.data_extract_threads import data_extract
from spider.first_layer_url_generate_threads import first_layer_url_generator, \
    run_formal_spider_conf
from spider.file_extract_threads import file_extract
from spider.data_deduplicate_threads import query_guid_existed_mongo

app = FastAPI()

# 设置允许访问的域名
origins = ["*"]  # 也可以设置为"*"，即为所有。

# 设置跨域传参
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # 设置允许的origins来源
    allow_credentials=True,
    allow_methods=["*"],  # 设置允许跨域的http方法，比如 get、post、put等。
    allow_headers=["*"])  # 允许跨域的headers，可以用来鉴别来源等作用。


# LOGGER = logging.getLogger("spider.api")


class Item(BaseModel):
    task_type: str
    sourceid: Optional[str] = ''


class Item2(BaseModel):
    sourceid: str
    task_type: str


class Item3(BaseModel):
    task_type: str
    sourceid: Optional[str] = ''
    data: Optional[dict] = {}


class Item4(BaseModel):
    task_type: str
    sourceid: Optional[str] = ''
    # url: Optional[str] = ''
    # total_layers: Optional[int] = 0
    # current_layer: Optional[int] = 0
    data: Optional[dict] = {}


class Item5(BaseModel):
    source_id_list: Optional[list] = []


class Item6(BaseModel):
    guid_str: str


class Item7(BaseModel):
    sourceid: str
    modulecode: int
    guid_str: str


@app.get('/')
async def root():
    return {"message": "Have a nice day💜"}


@app.get('/urls')
async def urls_test_get():
    return {"message": "This is get-first-urls-run-API and have a nice day💜"}


@app.post('/urls')
async def urls_test_post(item: Item):
    try:
        json_item = jsonable_encoder(item)
        data = first_layer_url_generator(json_item)
    except Exception as e:
        return JSONResponse({
            "state": 0,
            "data": [],
            "error": e.args[0],
        }, status_code=500)
    else:
        return {
            "state": 1,
            "data": data,
            "error": "",
        }


@app.get('/data')
async def data_test_get():
    return {"message": "This is get-data-API and have a nice day💜"}


@app.post('/data')
async def data_test_post(item: Item4):
    try:
        json_item = jsonable_encoder(item)
        data_param = json_item.get('data', '')
        sourceid = json_item.get('sourceid', '')
        task_type = json_item.get('task_type', '')
        if data_param:
            json_item.pop('data')
            json_item.update(data_param)
            # total_layers = int(json_item.get('total_layers', 0))
            # current_layer = int(json_item.get('current_layer', 0))
            # if total_layers > 2 and current_layer < total_layers - 1:
            #     data = multi_layer_spider(json_item)
            # else:
            #     data = second_to_last_layer_spider(json_item)
            res = data_download(json_item)
            if res is None:
                # data = [{"_url": '网址请求失败!'}]
                final_data = JSONResponse({
                    "state": 0,
                    "data": [],
                    "error": "网址请求失败!",
                }, status_code=500)
            else:
                if not res.get('all_layer_data', ''):
                    res['all_layer_data'] = data_param
                # data = data_extract(res)
                final_data = {
                    "state": 1,
                    "data": data_extract(res),
                    "error": "",
                }
                extract_data_0 = final_data['data'][0]
                extract_data_list_new = []
                if extract_data_0.get('content_is_auto_file', ''):
                    # 正文需要进行附件抽取
                    new_dict = {
                        "_url": extract_data_0["_url"],
                        "_content_final": extract_data_0["_content_final"],
                    }
                    for d in extract_data_0["fields"]:
                        # 开始附件抽取
                        file_info_list = file_extract(
                            {"sourceid": sourceid, 'task_type': task_type,
                             "stored_fields": d,
                             "modulecode": extract_data_0["modulecode"],
                             "grabdatacode": extract_data_0["grabdatacode"],
                             "content_is_auto_file": extract_data_0['content_is_auto_file']})

                        if isinstance(file_info_list, dict):
                            # 没抽取到附件，原样返回
                            new_dict["fields"] = d["fields"]
                            extract_data_list_new.append(new_dict)
                        elif isinstance(file_info_list, list):
                            for file_info in file_info_list:
                                final_d = {}
                                final_d.update(new_dict)
                                final_d["fields"] = [file_info]
                                extract_data_list_new.append(final_d)
                        else:
                            raise Exception('附件抽取有问题！')

                final_data['data'] = extract_data_list_new if extract_data_list_new else final_data[
                    'data']
        else:
            # data = [{"_url": '请求参数有误，未包含有效参数data!'}]
            final_data = JSONResponse({
                "state": 0,
                "data": [],
                "error": "请求参数有误，未包含有效参数data!!",
            }, status_code=500)
    except Exception as e:
        return JSONResponse({
            "state": 0,
            "data": [],
            "error": e.args[0],
        }, status_code=500)
    return final_data


@app.get('/run')
async def run_spider_test_get():
    return {"message": "This is run-API and have a nice day💜"}


@app.post('/run-formal-spider-conf')
async def run_spider_formal_post(item: Item5, backgroundtask: BackgroundTasks):
    try:
        json_item = jsonable_encoder(item)
        backgroundtask.add_task(run_formal_spider_conf, json_item)
    except Exception as e:
        print(e)
        return JSONResponse({
            "state": 0,
            "data": [],
            "error": e.args[0],
        }, status_code=500)
    else:
        return {
            "state": 1,
            "data": [{"message": "start spiders in the background"}],
            "error": "",

        }


@app.post('/run')
async def run_spider_test_post(item: Item2, backgroundtask: BackgroundTasks):
    try:
        json_item = jsonable_encoder(item)

        backgroundtask.add_task(first_layer_url_generator, json_item)
        # backgroundtask.add_task(go_list, json_item)
        # backgroundtask.add_task(go_content, json_item)
        # goList_p(json_item)  # 目录
    except Exception as e:
        print(e)
        return JSONResponse({
            "state": 0,
            "data": [],
            "error": e.args[0],
        }, status_code=500)
    else:
        return {
            "state": 1,
            "data": [{"message": f"{len(backgroundtask.tasks)} spiders in the background"}],
            "error": "",

        }


@app.post('/generateGuid')
async def generate_data_guid_post(item: Item6):
    try:
        guid_dict = jsonable_encoder(item)
        if not guid_dict.get('guid_str', ''):
            raise Exception(
                f'手动生成guid - 报错：guid文本数据不得为空!')
        data_guid = OhMyExtractor.generate_uniqueGuid('http://' + guid_dict['guid_str'], '_url')
    except Exception as e:
        return JSONResponse({
            "state": 0,
            "data": [],
            "error": e.args[0],
        }, status_code=500)
    return {
        "state": 1,
        "data": data_guid,
        "error": "",
    }


@app.post('/queryGuidExistedMongo')
async def query_guid_existed_mongo_post(item: Item7):
    try:
        json_item = jsonable_encoder(item)
        data = query_guid_existed_mongo(json_item)
    except Exception as e:
        return JSONResponse({
            "state": 0,
            "data": [],
            "error": e.args[0],
        }, status_code=500)
    return {
        "state": 1,
        "data": data,
        "error": "",
    }


if __name__ == "__main__":
    import uvicorn

    macip = OhMyExtractor.get_ip()
    print(f'当前服务器ip为：{macip}')
    uvicorn.run(app='spider_api_new:app', host=macip, port=8050)
