#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from liquiclient.config import get_property, get_property_or_none
from pyiceberg.catalog import load_catalog


# 获取iceberg catalog实例
def get_iceberg_client():
    catalog_name = get_property("iceberg.catalog.name")
    catalog_type = get_property("iceberg.catalog.type")
    catalog_uri  = get_property("iceberg.catalog.uri")
    warehouse    = get_property("iceberg.catalog.warehouse")

    config = {
        "type": catalog_type,
        "uri": catalog_uri,
        "warehouse": warehouse,
    }

    # 可选: S3 认证信息
    s3_endpoint = get_property_or_none("iceberg.s3.endpoint")
    if s3_endpoint:
        config["s3.endpoint"] = s3_endpoint
        config["s3.access-key-id"] = get_property("iceberg.s3.access-key-id")
        config["s3.secret-access-key"] = get_property("iceberg.s3.secret-access-key")

    return load_catalog(catalog_name, **config)


# 获取iceberg集群catalog实例
def get_iceberg_cluster_client(cluster):
    prefix = cluster + ".iceberg"
    catalog_name = get_property(prefix + ".catalog.name")
    catalog_type = get_property(prefix + ".catalog.type")
    catalog_uri  = get_property(prefix + ".catalog.uri")
    warehouse    = get_property(prefix + ".catalog.warehouse")

    config = {
        "type": catalog_type,
        "uri": catalog_uri,
        "warehouse": warehouse,
    }

    s3_endpoint = get_property_or_none(prefix + ".s3.endpoint")
    if s3_endpoint:
        config["s3.endpoint"] = s3_endpoint
        config["s3.access-key-id"] = get_property(prefix + ".s3.access-key-id")
        config["s3.secret-access-key"] = get_property(prefix + ".s3.secret-access-key")

    return load_catalog(catalog_name, **config)
