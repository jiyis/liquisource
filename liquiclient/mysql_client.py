#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from liquiclient.config import get_property
from urllib.parse import urlparse
from urllib.parse import parse_qs
import mysql.connector


# 获取mysql实例
def get_mysql_client():
    # 由于默认liquibase是jdbc的，这里解析对应的ip port
    params = parse_jdbc_dsn(get_property("url"))
    client = mysql.connector.connect(**params)

    return client


def parse_jdbc_dsn(dsn):
    url_str = dsn.lstrip("jdbc:mysql://")
    # 如果URL没有scheme，为其添加默认的http或https
    if not url_str.startswith(('http://', 'https://')):
        url_str = 'http://' + url_str

    # 当成url解析
    url_obj = urlparse(url_str)
    query_params = parse_qs(url_obj.query)

    # 获取账号密码
    username = get_property("username")
    password = get_property("password")

    config = {
        "host": url_obj.hostname,
        "port": url_obj.port,
        "database": url_obj.path.lstrip("/"),
        "user": username,
        "password": password,
        "charset": query_params['characterEncoding'][0],
    }

    return config
