#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import mysql.connector
from liquiclient.config import get_property, get_property_or_none


# 获取starrocks实例
def get_starrocks_client():
    host     = get_property("starrocks.host")
    port     = int(get_property("starrocks.port"))
    username = get_property("starrocks.username")
    password = get_property("starrocks.password")


    conn_params = {
        "host": host, "port": port, "user": username
    }
    if password:
        conn_params["password"] = password

    return mysql.connector.connect(**conn_params)


# 获取starrocks集群实例
def get_starrocks_cluster_client(cluster):
    prefix   = cluster + ".starrocks"
    host     = get_property(prefix + ".host")
    port     = int(get_property(prefix + ".port"))
    username = get_property(prefix + ".username")
    password = get_property(prefix + ".password")


    conn_params = {
        "host": host, "port": port, "user": username
    }
    if password:
        conn_params["password"] = password

    return mysql.connector.connect(**conn_params)
