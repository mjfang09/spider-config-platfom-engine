#!/usr/bin/env python
# -*- encoding: utf-8 -*-
"""
@Project ：  spider-nav-config-platform
@File    :   run_cron.py
@Time    :   2024/03/13 14:40
@Author  :   MengJia_Fang 501738
@E-mail  :   fangmj@finchina.com
@License :   (C)Copyright Financial China Information & Technology Co., Ltd.
@Desc    :   Description
@Version :   1.0
"""

# here put the import lib
import pathlib
import re
import time
import json
import pyodbc
from loguru import logger
from tzlocal import get_localzone
from apscheduler.triggers.cron import CronTrigger
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import (
    EVENT_JOB_EXECUTED,
    EVENT_JOB_ERROR,
    EVENT_JOB_ADDED,
    EVENT_JOB_MODIFIED,
    EVENT_JOB_REMOVED,
    EVENT_JOB_MISSED,
)
from spider_logging import kafka_logging
from spider_logging.kafkalogger import KafkaLogger
from spider_logging.kafka_logging import listener, start_hear_beat
from utils.mqUtil import OhMyMqSender
import utils.env

# 数据库环境 test/formal
db_env = utils.env.DB_ENV
# 消息队列环境 test/formal
mq_env = utils.env.MQ_ENV
LOG_APP_ID = ''
LOGGER_NAME = 'spider.CronList'
LC_JZ_CRON_TASK_QUEUE_NAME = 'lc_jz_cron_task'
kafka_logging.__logInit__ = True
LOGGER = KafkaLogger(name=LOGGER_NAME, appid=LOG_APP_ID)


class LogUtils:
    log_file_path = f"./cron_schedule_logs/{pathlib.Path(__file__).stem}_schedule.log"
    p = pathlib.Path(log_file_path)
    if not p.parent.exists():
        p.parent.mkdir()
    # logger.add(log_file_path, rotation='00:00', retention='30 days', encoding='utf-8')
    logger.add(log_file_path, rotation='00:00', enqueue=True, encoding='utf-8')


scheduler = BackgroundScheduler(
    {
        "apscheduler.jobstores.default": {
            "type": "sqlalchemy",
            "url": "sqlite:///jobs.sqlite",
        },
        "apscheduler.executors.default": {
            "class": "apscheduler.executors.pool:ThreadPoolExecutor",
            "max_workers": "20",
        },
        "apscheduler.job_defaults.coalesce": "true",
        "apscheduler.job_defaults.misfire_grace_time": "300",
        "apscheduler.job_defaults.max_instances": "1",
        "apscheduler.timezone": get_localzone(),
    }
)

dev_db = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=10.17.207.185,1433;DATABASE=RREPORTTASK;UID=dbinput;PWD=123456"
pro_db = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=10.17.205.164,1433;DATABASE=RREPORTTASK;UID=apphct;PWD=apphct"


class MyCronTrigger(CronTrigger):
    def __eq__(self, __o: object) -> bool:
        if not isinstance(__o, MyCronTrigger):
            return NotImplemented

        return (
                self.fields == __o.fields
        )


def my_job(record):
    LOGGER.shd_begin()
    # print(record)
    try:
        print(record)
        id = record['id']
        if record.get("tmstamp", None):
            del record['tmstamp']
        if record.get("entrydate", None):
            del record['entrydate']
        if record.get("entrytime", None):
            del record['entrytime']

        # queueName = record['queuename']
        record = json.dumps(record, ensure_ascii=False)
        # if not queueName:
        #     queueName = LC_JZ_LIST_HIS_QUEUE_NAME
        # queueName = 'lcgg_list_' + queueName
        try:
            send = OhMyMqSender(queue_name=LC_JZ_CRON_TASK_QUEUE_NAME, priority=15,
                                mq_env=mq_env)  # 历史数据消息等级2 ，日常消息等级15
            send.send_json(record)
            LOGGER.other_op.info(f'''{id}--推送成功！''')

        except Exception as e:
            LOGGER.other_op.info(f'''{id}--推送失败！- {e}''')
        # with pyodbc.connect(pro_db) as conn:
        #     with conn.cursor() as cursor:
        #         cursor.execute(
        #             f"UPDATE RREPORTTASK..ct_litigationsource set polling_num=0 where id = {taskID}"
        #         )
        #         conn.commit()

        # LOGGER.other_op.info(f"ID[{id}] - 分发状态已重置")
    except Exception as e:
        # conn.rollback()
        LOGGER.other_op.info(f"{id} - 分发状态重置失败")

    LOGGER.shd_end()


def job_listener(event):
    if event.code == EVENT_JOB_ERROR:
        logger.error(f"Job[{event.job_id}] error - {event.exception}")
    elif event.code == EVENT_JOB_MISSED:
        logger.error(f"Job[{event.job_id}] missed - {event.exception}")
    elif event.code == EVENT_JOB_EXECUTED:
        logger.info(f"Job[{event.job_id}] success")
    elif event.code == EVENT_JOB_ADDED:
        logger.info(f"Job[{event.job_id}] added")
    elif event.code == EVENT_JOB_MODIFIED:
        logger.info(f"Job[{event.job_id}] modified")
    elif event.code == EVENT_JOB_REMOVED:
        logger.info(f"Job[{event.job_id}] removed")


def run():
    with pyodbc.connect(pro_db) as conn:
        with conn.cursor() as cursor:
            # cursor.execute(
            #     "SELECT a.id id_,a.sourceid sourceid_,a.grabdatacode grabdatacode_,a.url url_,a.guid guid_,a.queuename queuename_,b.* FROM  RReportTask..ct_nav_list_url a inner join RReportTask..ct_finprod_nav_config b on a.sourceid=b.sourceid where b.state=1")
            cursor.execute(
                "select * FROM RReportTask..ct_finprod_nav_config  WHERE state=1 and use_rules!=0 order by id desc")
            records = [
                dict(zip([column[0] for column in cursor.description], row))
                for row in cursor.fetchall()
            ]

            for record in records:
                taskID = record["sourceid"]
                id = record["id"]
                taskcron = record["crontab"]
                if not taskcron:
                    print(taskID, " ", taskcron, 'crontab为空不处理')
                    continue
                print(taskID, " ", taskcron)
                try:
                    crontab = taskcron
                    crontab = re.sub(r'\s+', ' ', crontab)
                    crontab = re.sub(r'([\s\S]*?)\s$', '\\1', crontab)
                    crontab = re.sub(r'^(\S*?)\s(\S*?\s\S*?\s\S*?\s\S*?\s\S*?$)', '\\2 \\1',
                                     crontab)

                    crontab = re.sub(r'^(\S*?)\s(\S*?\s\S*?\s\S*?\s\S*?$)', '\\2 * \\1', crontab)
                    crontab = re.sub(r'^(\S*?)\s(\S*?\s\S*?\s\S*?\s\S*?\s\S*?)\s\S*?$', '\\2 \\1',
                                     crontab)  # 去除“年”
                    cron_hour = re.search(r'^(\S*?\s)(\d{1,})-(\d{1,})(\s\S*?\s\S*?\s\S*?\s\S*?)$',
                                          crontab)  # hour处理
                    if cron_hour and int(cron_hour[2]) > int(cron_hour[3]):
                        start_hour = str(cron_hour[2]) + '-23'
                        end_hour = '0-' + str(cron_hour[3])
                        crontab = cron_hour[1] + start_hour + ',' + end_hour + cron_hour[4]

                    cron_hour1 = re.search(
                        r'^(\S*?\s)(\d+,)(\d{1,})-(\d{1,})(\s\S*?\s\S*?\s\S*?\s\S*?)$',
                        crontab)  # hour处理 '0 17/1 16,22-10 * * ?'
                    if cron_hour1 and int(cron_hour1[3]) > int(cron_hour1[4]):
                        start_hour = str(cron_hour1[3]) + '-23'
                        end_hour = '0-' + str(cron_hour1[4])
                        crontab = cron_hour1[1] + cron_hour1[2] + start_hour + ',' + end_hour + \
                                  cron_hour1[5]
                    cron_week = re.search(r'^(\S*?\s\S*?)(\s\?)(\s\S*?)(\s\d\S*?)(\s\S*?)$',
                                          crontab)  # week处理 quartz 1（周日） 2（周一） 3（周二） 4（周三） 5（周四） 6（周五） 7（周六）
                    weeks = {"0": "Sun",
                             "1": "Mon",
                             "2": "Tue",
                             "3": "Wed",
                             "4": "Thu",
                             "5": "Fri",
                             "6": "Sat",
                             "7": "Sun"}
                    if cron_week:
                        if '1-7' in cron_week[4]:
                            crontab = cron_week[1] + cron_week[2] + cron_week[3] + ' ' + '*' + \
                                      cron_week[5]

                        elif '-' in cron_week[4]:
                            weekmatch = re.search(r'\s(\d)-(\d)$', cron_week[4])
                            sweek = int(weekmatch[1]) - 1
                            sweek = weeks[str(sweek)]
                            eweek = int(weekmatch[2]) - 1
                            eweek = weeks[str(eweek)]
                            weekrange = ' ' + str(sweek) + '-' + str(eweek)
                            crontab = cron_week[1] + cron_week[2] + cron_week[3] + weekrange + \
                                      cron_week[5]
                        elif ',' in cron_week[4]:
                            weekmatch = re.search(r'\s(\d),(\d)$', cron_week[4])
                            sweek = int(weekmatch[1]) - 1
                            sweek = weeks[str(sweek)]
                            eweek = int(weekmatch[2]) - 1
                            eweek = weeks[str(eweek)]

                            weekrange = ' ' + str(sweek) + ',' + str(eweek)
                            crontab = cron_week[1] + cron_week[2] + cron_week[3] + weekrange + \
                                      cron_week[5]
                        else:

                            weekend = str(int(cron_week[4]) - 1)
                            weekend = weeks[str(weekend)]
                            weekend = ' ' + weekend
                            crontab = cron_week[1] + cron_week[2] + cron_week[3] + weekend + \
                                      cron_week[5]
                    crontab = re.sub(r'^(\S*?\s\S*?\s\S*?\s\S*?\s\S*?)(\s0)$', '\\1', crontab)
                    crontab = re.sub(r'\?', '*', crontab)
                    record["crontab"] = crontab
                    # print(taskID, " ", crontab)
                    # crontab = " ".join(record["crontab"].split(" ")[1:]).replace(
                    #     "?", "*"
                    # )
                except:
                    continue

                if myjob := scheduler.get_job(id):
                    scheduler.modify_job(
                        id, trigger=MyCronTrigger.from_crontab(crontab)
                    )

                else:
                    myjob = scheduler.add_job(
                        func=my_job,
                        trigger=MyCronTrigger.from_crontab(crontab),
                        id=str(record["id"]),
                        name=str(taskID),
                        max_instances=1,
                        replace_existing=True,
                        args=[record],
                    )

            running_jobs = [record["id"] for record in records]
            for myjob in scheduler.get_jobs():
                if int(myjob.id) not in running_jobs:
                    scheduler.remove_job(myjob.id)


if __name__ == '__main__':
    start_hear_beat(app_id=LOG_APP_ID)
    listener.start()
    LOGGER.other_op.info("调度开始")

    try:
        scheduler.add_listener(
            job_listener,
            EVENT_JOB_EXECUTED
            | EVENT_JOB_ERROR
            | EVENT_JOB_ADDED
            | EVENT_JOB_MODIFIED
            | EVENT_JOB_REMOVED
            | EVENT_JOB_MISSED,
        )
        scheduler.start()
        while True:
            run()
            time.sleep(60 * 10)

    except (KeyboardInterrupt, SystemExit):
        pass

    LOGGER.other_op.info("分发调度结束")
    listener.stop()
