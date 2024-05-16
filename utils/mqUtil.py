#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   mqUtil.py
@Time    :   2020/12/22 09:36:56
@Author  :   Jianhua Ye
@Phone    :   15655140926
@E-mail :   yejh@finchina.com
@License :   (C)Copyright Financial China Information & Technology Co., Ltd.
@Desc    :   消息类
@Version :   1.0
'''

# here put the import lib
import json
import random
import time
import traceback
import uuid
from typing import Union

import pika
from pika import BlockingConnection
from pika.exceptions import ConnectionClosed, ChannelWrongStateError, StreamLostError, \
    ChannelClosedByBroker, IncompatibleProtocolError, ConnectionClosedByBroker

import utils.settings
from utils.tools import OhMyExtractor


class RabbitMqManager(object):
    """
    @function:
    """

    def __init__(self, mq_env='dev'):
        self.mq_env = mq_env
        conf = utils.settings.MQ_INFO.get(self.mq_env)
        self.username = conf['MQ_USER']
        self.password = conf['MQ_PSWD']
        self.hosts = conf['MQ_HOST']
        self.port = conf['MQ_PORT']
        _userinfo = pika.PlainCredentials(self.username, self.password)

        if isinstance(self.hosts, list):
            # 重排链接优先级
            self.all_endpoints = [
                pika.ConnectionParameters(host=host, port=self.port, connection_attempts=5,
                                          retry_delay=1,
                                          heartbeat=0,
                                          credentials=_userinfo) for host in self.hosts]
            random.shuffle(self.all_endpoints)

        else:
            self.all_endpoints = (
                pika.ConnectionParameters(host=self.hosts, port=self.port, connection_attempts=5,
                                          retry_delay=1,
                                          credentials=_userinfo))

        self._connection = pika.BlockingConnection(self.all_endpoints)
        self._channel = self._connection.channel()

    def queue_declare(self, queue_name, x_max_priority=None, send_confirm: bool = True,
                      prefetch_count: Union[None, int] = None, bind_dead_queue=False):
        if send_confirm:
            self._channel.confirm_delivery()
        if prefetch_count:
            self._channel.basic_qos(prefetch_count=prefetch_count)
        arguments = {}
        if x_max_priority:
            arguments["x-max-priority"] = x_max_priority
        if bind_dead_queue:
            arguments["x-dead-letter-exchange"] = 'dead_exchange_' + queue_name
            arguments["x-dead-letter-routing-key"] = 'dead_' + queue_name
            arguments["x-max-attempts"] = 3
            self._channel.exchange_declare(exchange='dead_exchange_' + queue_name,
                                           exchange_type='direct', durable=True)
            self._channel.queue_declare(queue='dead_' + queue_name, durable=True)
            # 绑定死信交换器和死信队列
            self._channel.queue_bind(exchange='dead_exchange_' + queue_name,
                                     queue='dead_' + queue_name, routing_key='dead_' + queue_name)

        self._channel.queue_declare(queue=queue_name, durable=True, arguments=arguments)

    def exchange_declare(self, exchange, x_max_priority=None, confirm: bool = True,
                         exchange_type: str = 'topic'):
        if confirm:
            self._channel.confirm_delivery()
        arguments = {}
        if x_max_priority:
            arguments["x-max-priority"] = x_max_priority
        self._channel.exchange_declare(exchange=exchange, exchange_type=exchange_type, durable=True,
                                       arguments=arguments)

    def send_json_msg(self, queue_name, json_str, exchange='', priority=0):
        for i in range(3):
            try:
                if not self._connection or self._connection.is_closed or not self._channel or self._channel.is_closed:
                    self.reset_connection()
                self._channel.basic_publish(exchange=exchange,
                                            routing_key=queue_name,
                                            body=str(json_str),
                                            properties=pika.BasicProperties(
                                                delivery_mode=pika.DeliveryMode.Persistent,
                                                priority=priority))
                # # 添加确认监听
                # self._channel.add_on_delivery_callback(self.callback)
            except Exception as e:
                print(e)
                self.reset_connection()
            else:
                return True
        raise Exception("LostConnection")

    def consume(self, queue_name, prefetch_count, on_message_callback, auto_ack=False):
        """
        exception: 链接失败时重置链接
        :param prefetch_count:
        :param queue_name:
        :param on_message_callback:
        :param auto_ack:
        :return:
        """
        try:
            self._channel.basic_qos(prefetch_count=prefetch_count)
            if not self._connection or self._connection.is_closed or not self._channel or self._channel.is_closed:
                self.reset_connection()

            self._channel.basic_consume(queue=queue_name,
                                        on_message_callback=on_message_callback,
                                        auto_ack=auto_ack,
                                        consumer_tag=OhMyExtractor.get_ip() + '.' + uuid.uuid4().hex)
        except (ConnectionClosed, ChannelWrongStateError):
            self.reset_connection()

    # def publish(self, routing_key: str = "", exchange: str = '', data: str = None, json: dict = None,
    #             priority: Optional[int] = 15, **kwargs):
    #     """
    #     发送到队列,json比data更优先
    #     :param routing_key: 路由绑定
    #     :param exchange: 交换机
    #     :param data: str数据
    #     :param json: 待发送数据，将被接json序列化后发送
    #     :param priority: 优先级,历史数据消息等级2, 日常消息等级15, 非优先级队列该参数无效
    #     :return: None
    #     :raise MessageSendError:
    #     """
    #
    #     if not self._channel:
    #         raise ConnectionError("未连接channel")
    #     if json:
    #         data = complex_json.dumps(json, ensure_ascii=False)
    #
    #     # 尝试发送消息三次
    #     retry_times = 3
    #     while retry_times:
    #         try:
    #             self._channel.basic_publish(exchange=exchange,
    #                                         routing_key=routing_key,
    #                                         body=data.encode("utf8"),
    #                                         properties=BasicProperties(delivery_mode=2,  # 2-持久化
    #                                                                    priority=priority,
    #                                                                    **kwargs))
    #             return
    #         # 只处理链接丢失相关的错误
    #         except (StreamLostError, ChannelClosedByBroker, IncompatibleProtocolError, ConnectionClosedByBroker):
    #             self.reset_connection()
    #             LOG.warning("检测到rabbit mq 链接丢失，重新链接")
    #             retry_times -= 1
    #
    #     LOG.error("消息发送失败: " + str(data))
    #     raise MessageSendError("消息发送失败, 请检查消息服务器是否存活")

    def reset_connection(self):
        """
        重置链接
        :return:
        """
        try:
            self._connection = BlockingConnection(self.all_endpoints)
            self._channel = self._connection.channel()
        except (IncompatibleProtocolError, ConnectionClosedByBroker, StreamLostError):
            print(f"连接异常：{traceback.format_exc()}")
        print("重新连接")
        time.sleep(1)

    def close(self):
        try:
            if self._channel:
                self._channel.close()
            if self._connection:
                self._connection.close()
        except Exception as e:
            print(f"关闭连接异常：{str(traceback.format_exc())}")

    # def get_message_info(self, queue_name: str):
    #     """
    #     获取指定队列详细信息
    #     :param queue_name
    #     :return
    #     """
    #     url = f'http://{self.conf.host[0]}:{self.conf.adminPort}/api/queues/%2F/{queue_name}'
    #     LOG.info("rabbit mq admin %s", url)
    #     try:
    #         response = requests.get(url, auth=(self.conf.username, self.conf.password))
    #         if response.status_code != 200:
    #             raise Exception(f'队列消息数获取失败 - {response.status_code}')
    #     except Exception as e:
    #         raise Exception(f'队列消息数获取失败 - {e}')
    #     else:
    #         return response.json()

    def __repr__(self):
        return f"RabbitMQService(env={self.mq_env})"

    #
    # def consumer(self,
    #              queue_name: str,
    #              on_message_callback: Callable[
    #                  [BlockingChannel, spec.Basic.Deliver, spec.BasicProperties, bytes],
    #                  None],
    #              auto_ack: bool,
    #              prefetch_count: int = 1):
    #     """
    #     exception: 链接失败时重置链接
    #     :param queue_name: 监听的队列
    #     :param on_message_callback:
    #     :param auto_ack:
    #     :param prefetch_count:
    #     :return:
    #     """
    #     self._channel.basic_qos(prefetch_count=prefetch_count)
    #     try:
    #         self._channel.basic_consume(queue=queue_name,
    #                                     on_message_callback=on_message_callback,
    #                                     auto_ack=auto_ack,
    #                                     consumer_tag=get_ip() + '.' + uuid.uuid4().hex)
    #     except (ConnectionClosed, ChannelWrongStateError):
    #         self.reset_connection()

    def start(self):
        """
        启动消费者
        :return:
        """
        while True:
            try:
                self._channel.start_consuming()
            except (StreamLostError, ChannelClosedByBroker, IncompatibleProtocolError,
                    ConnectionClosedByBroker):
                self.reset_connection()
                print("检测到rabbit mq 链接丢失，重新链接")

    def start_consume(self):
        """
        启动消费者
        :return:
        """
        try:
            self._channel.start_consuming()
        except (StreamLostError, ChannelClosedByBroker, IncompatibleProtocolError,
                ConnectionClosedByBroker):
            self.reset_connection()
            raise Exception("检测到rabbit mq 链接丢失，重新链接")


def main():
    pass


if __name__ == "__main__":
    main()
