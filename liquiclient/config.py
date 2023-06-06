#!/usr/local/bin/python3
# coding: utf8
import sys
from jproperties import Properties
import os

config = Properties()


# 获取配置信息
def get_config(filepath):
    with open(filepath, 'rb') as prop:
        config.load(prop)


# 默认找项目路径的文件
def auto_config():
    # 获取当前工作目录
    current_working_directory = os.getcwd()

    # 获取父级目录
    parent_directory = os.path.dirname(current_working_directory)

    # 在父级目录中拼接给定的文件名
    file_path = os.path.join(parent_directory, "liquibase.properties")
    with open(file_path, 'rb') as prop:
        config.load(prop)


# 获取配置的某一个key
def get_property(name):
    if name == "":
        return config
    return config[name].data


# 获取租户id
def get_tenant():
    return get_property("parameter.tenant")


# 获取空间id
def get_biz():
    return get_property("parameter.biz")


# 获取根据租户分库分表
def get_tenant_shard(key):
    kfuin = get_property("parameter.tenant")
    return key + "_" + kfuin


auto_config()
