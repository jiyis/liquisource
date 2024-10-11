#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import redis
from liquiclient.config import get_property
from redis.cluster import RedisCluster


# 获取redis实例
def get_redis_client():
    mode = get_property("redis.mode")
    if mode != "cluster":
        host = get_property("redis.host")
        port = get_property("redis.port")
        username = get_property("redis.username")
        password = get_property("redis.password")
        client = redis.Redis(host=host, port=port, username=username, password=password, decode_responses=True)
    else:
        url = get_property("redis.host")
        client = RedisCluster.from_url(url, decode_responses=True)

    return client


# 获取redis实例
def get_redis_cluster_client(key):
    mode = get_property("redis.mode")
    if mode != "cluster":
        host = get_property(key + ".redis.host")
        port = get_property(key + ".redis.port")
        username = get_property(key + ".redis.username")
        password = get_property(key + ".redis.password")

        client = redis.Redis(host=host, port=port, username=username, password=password, decode_responses=True)
    else:
        url = get_property(key+"redis.host")
        client = RedisCluster.from_url(url, decode_responses=True)

    return client
