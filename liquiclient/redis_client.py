#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import redis
from liquiclient.config import get_property


# 获取mysql实例
def get_redis_client():

    host = get_property("redis.host")
    port = get_property("redis.port")
    username = get_property("redis.username")
    password = get_property("redis.password")

    client = redis.Redis(host=host, port=port, username=username, password=password, decode_responses=True)

    return client
