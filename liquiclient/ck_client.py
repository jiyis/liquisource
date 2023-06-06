#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import clickhouse_connect

from liquiclient.config import get_config, get_property


# 获取clickhouse实例
def get_ck_client(filepath):
    get_config(filepath)
    host = get_property("ck.host")
    port = get_property("ck.port")
    username = get_property("ck.username")
    password = get_property("ck.password")
    # 获取clickhouse链接实例
    client = clickhouse_connect.get_client(host=host, username=username,
                                           password=password)

    return client

