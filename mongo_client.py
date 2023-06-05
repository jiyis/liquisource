#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pymongo

from config import *


# 获取mongodb实例
def get_client(filepath):
    get_config(filepath)
    url = get_property("mongo.url")
    # 获取mongo链接实例
    client = pymongo.MongoClient(url)

    return client




