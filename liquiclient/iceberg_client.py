#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import threading

from pyiceberg.catalog import load_catalog
from pyspark.sql import SparkSession

from liquiclient.config import get_property, get_property_or_none

# ============================================================
# SparkSession 全局缓存（按 catalog 名称缓存，避免重复创建）
# ============================================================
_spark_sessions = {}
_lock = threading.Lock()


def _build_storage_config(prefix):
    """
    根据存储类型构建对应的配置参数
    支持: s3(AWS S3) / minio / cos(腾讯云COS) / oss(阿里云OSS)
    """
    s3_type = get_property_or_none(prefix + ".s3.type")
    if not s3_type:
        # 没有配置存储类型，尝试兼容旧的纯 S3 配置
        s3_endpoint = get_property_or_none(prefix + ".s3.endpoint")
        if s3_endpoint:
            return {
                "s3.endpoint": s3_endpoint,
                "s3.secret_key": get_property(prefix + ".s3.secret_key"),
                "s3.access_key": get_property(prefix + ".s3.access_key"),
            }
        return {}

    s3_type = s3_type.strip().lower()
    config = {}

    if s3_type == "s3":
        # AWS S3
        config["s3.endpoint"] = get_property(prefix + ".s3.endpoint")
        config["s3.secret_key"] = get_property(prefix + ".s3.secret_key")
        config["s3.access_key"] = get_property(prefix + ".s3.access_key")
        region = get_property_or_none(prefix + ".s3.region")
        if region:
            config["s3.region"] = region

    elif s3_type == "minio":
        # MinIO (S3兼容)
        config["s3.endpoint"] = get_property(prefix + ".s3.endpoint")
        config["s3.secret_key"] = get_property(prefix + ".s3.secret_key")
        config["s3.access_key"] = get_property(prefix + ".s3.access_key")
        config["s3.path_style"] = "true"

    elif s3_type == "cos":
        # 腾讯云 COS (cosn://)
        config["s3.endpoint"] = get_property(prefix + ".s3.endpoint")
        config["s3.secret_key"] = get_property(prefix + ".s3.secret_key")
        config["s3.access_key"] = get_property(prefix + ".s3.access_key")
        # COS Hadoop 文件系统配置
        config["fs.cosn.userinfo.secretId"] = get_property(prefix + ".s3.secret_key")
        config["fs.cosn.userinfo.secretKey"] = get_property(prefix + ".s3.access_key")
        config["fs.cosn.bucket.endpoint_suffix"] = get_property(prefix + ".s3.endpoint")
        config["fs.cosn.impl"] = "org.apache.hadoop.fs.CosFileSystem"
        config["fs.AbstractFileSystem.cosn.impl"] = "org.apache.hadoop.fs.CosN"

    elif s3_type == "oss":
        # 阿里云 OSS (oss://)
        config["s3.endpoint"] = get_property(prefix + ".s3.endpoint")
        config["s3.secret_key"] = get_property(prefix + ".s3.secret_key")
        config["s3.access_key"] = get_property(prefix + ".s3.access_key")
        # OSS Hadoop 文件系统配置
        config["fs.oss.accessKeyId"] = get_property(prefix + ".s3.secret_key")
        config["fs.oss.accessKeySecret"] = get_property(prefix + ".s3.access_key")
        config["fs.oss.endpoint"] = get_property(prefix + ".s3.endpoint")
        config["fs.oss.impl"] = "org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem"
        config["fs.oss.connection.secure.enabled"] = "false"
        config["fs.oss.connection.maximum"] = "2048"

    else:
        raise ValueError(f"不支持的存储类型: {s3_type}，可选值: s3, minio, cos, oss")

    return config


# 获取iceberg catalog实例
def get_iceberg_client():
    catalog_name = get_property("iceberg.catalog.name")
    catalog_type = get_property("iceberg.catalog.type")
    catalog_uri = get_property("iceberg.catalog.uri")
    warehouse = get_property("iceberg.catalog.warehouse")

    config = {
        "type": catalog_type,
        "uri": catalog_uri,
        "warehouse": warehouse,
    }

    # REST Catalog 认证配置
    credential = get_property_or_none("iceberg.catalog.credential")
    if credential:
        config["credential"] = credential
    token = get_property_or_none("iceberg.catalog.token")
    if token:
        config["token"] = token
    scope = get_property_or_none("iceberg.catalog.scope")
    if scope:
        config["scope"] = scope

    # 禁用 Credential Vending（pyiceberg 默认请求 vended-credentials，
    # 若 Polaris 服务端未配置存储凭证会报错，DDL 操作不需要此功能）
    config["header.X-Iceberg-Access-Delegation"] = ""

    # 根据存储类型构建配置
    storage_config = _build_storage_config("iceberg")
    config.update(storage_config)

    return load_catalog(catalog_name, **config)


# 获取iceberg集群catalog实例
def get_iceberg_cluster_client(cluster):
    prefix = cluster + ".iceberg"
    catalog_name = get_property(prefix + ".catalog.name")
    catalog_type = get_property(prefix + ".catalog.type")
    catalog_uri = get_property(prefix + ".catalog.uri")
    warehouse = get_property(prefix + ".catalog.warehouse")

    config = {
        "type": catalog_type,
        "uri": catalog_uri,
        "warehouse": warehouse,
    }

    # REST Catalog 认证配置
    credential = get_property_or_none(prefix + ".catalog.credential")
    if credential:
        config["credential"] = credential
    token = get_property_or_none(prefix + ".catalog.token")
    if token:
        config["token"] = token
    scope = get_property_or_none(prefix + ".catalog.scope")
    if scope:
        config["scope"] = scope

    # 禁用 Credential Vending
    config["header.X-Iceberg-Access-Delegation"] = ""

    # 根据存储类型构建配置
    storage_config = _build_storage_config(prefix)
    config.update(storage_config)

    return load_catalog(catalog_name, **config)


def _get_catalog_config(cluster=None):
    """
    获取 catalog 配置（供 PySpark 使用）

    参数:
        cluster: 集群名称，为 None 时使用默认配置

    返回:
        (catalog_name, config_dict) 元组
    """
    if cluster:
        prefix = cluster + ".iceberg"
    else:
        prefix = "iceberg"

    catalog_name = get_property(prefix + ".catalog.name")
    catalog_type = get_property(prefix + ".catalog.type")
    catalog_uri = get_property(prefix + ".catalog.uri")
    warehouse = get_property(prefix + ".catalog.warehouse")

    config = {
        "type": catalog_type,
        "uri": catalog_uri,
        "warehouse": warehouse,
    }

    # REST Catalog 认证配置
    credential = get_property_or_none(prefix + ".catalog.credential")
    if credential:
        config["credential"] = credential
    token = get_property_or_none(prefix + ".catalog.token")
    if token:
        config["token"] = token
    scope = get_property_or_none(prefix + ".catalog.scope")
    if scope:
        config["scope"] = scope

    # 禁用 Credential Vending
    config["header.X-Iceberg-Access-Delegation"] = ""

    # 根据存储类型构建配置
    storage_config = _build_storage_config(prefix)
    config.update(storage_config)

    return catalog_name, config


# ============================================================
# PySpark SparkSession 管理
# ============================================================

def _build_spark_session(catalog_name, catalog_config):
    """
    构建配置了 Iceberg Catalog 的 SparkSession

    参数:
        catalog_name: catalog 名称，将作为 Spark SQL 中的 catalog 前缀
        catalog_config: dict，包含 catalog 连接配置

    返回:
        SparkSession 实例
    """
    # Iceberg Spark Runtime JAR（首次运行会自动从 Maven 下载）
    iceberg_version = "1.7.1"
    iceberg_spark_jar = f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{iceberg_version}"

    builder = SparkSession.builder \
        .appName(f"iceberg-{catalog_name}") \
        .config("spark.jars.packages", iceberg_spark_jar) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")

    # 设置 catalog 类型
    catalog_type = catalog_config.get("type", "rest")
    builder = builder.config(f"spark.sql.catalog.{catalog_name}.type", catalog_type)

    # 设置 catalog URI
    uri = catalog_config.get("uri")
    if uri:
        builder = builder.config(f"spark.sql.catalog.{catalog_name}.uri", uri)

    # 设置 warehouse
    warehouse = catalog_config.get("warehouse")
    if warehouse:
        builder = builder.config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse)

    # REST Catalog 认证配置
    credential = catalog_config.get("credential")
    if credential:
        builder = builder.config(f"spark.sql.catalog.{catalog_name}.credential", credential)

    token = catalog_config.get("token")
    if token:
        builder = builder.config(f"spark.sql.catalog.{catalog_name}.token", token)

    scope = catalog_config.get("scope")
    if scope:
        builder = builder.config(f"spark.sql.catalog.{catalog_name}.scope", scope)

    # header 配置（如禁用 Credential Vending）
    for key, value in catalog_config.items():
        if key.startswith("header."):
            builder = builder.config(f"spark.sql.catalog.{catalog_name}.{key}", value)

    # S3 / 存储相关配置
    for key, value in catalog_config.items():
        if key.startswith("s3.") or key.startswith("fs."):
            builder = builder.config(f"spark.sql.catalog.{catalog_name}.{key}", value)

    # 设置默认 catalog
    builder = builder.config("spark.sql.defaultCatalog", catalog_name)

    spark = builder.getOrCreate()
    return spark


def get_spark_session(catalog_name, catalog_config):
    """
    获取或创建 SparkSession（线程安全，按 catalog_name 缓存）

    参数:
        catalog_name: catalog 名称
        catalog_config: catalog 配置字典

    返回:
        SparkSession 实例
    """
    with _lock:
        if catalog_name in _spark_sessions:
            session = _spark_sessions[catalog_name]
            # 检查 session 是否仍然有效
            try:
                session.sql("SELECT 1")
                return session
            except Exception:
                # session 已失效，重新创建
                del _spark_sessions[catalog_name]

        session = _build_spark_session(catalog_name, catalog_config)
        _spark_sessions[catalog_name] = session
        return session


# ============================================================
# Iceberg SQL 执行（基于 PySpark）
# ============================================================

def execute_iceberg_sql(sql, cluster=None):
    """
    通过 PySpark 执行 Iceberg SQL 语句

    支持 Spark SQL 的所有 Iceberg 语法，包括但不限于:
      - CREATE TABLE / CREATE TABLE IF NOT EXISTS
      - DROP TABLE / DROP TABLE IF EXISTS
      - ALTER TABLE (ADD/DROP/RENAME COLUMN, SET TBLPROPERTIES, ...)
      - INSERT INTO / INSERT OVERWRITE
      - SELECT / MERGE INTO
      - CREATE/DROP NAMESPACE/SCHEMA/DATABASE

    参数:
        sql: SQL 语句字符串（Spark SQL 语法）
        cluster: 集群名称，为 None 时使用默认 catalog

    返回:
        pyspark DataFrame（查询结果）

    示例:
        execute_iceberg_sql('''
            CREATE TABLE IF NOT EXISTS my_db.users (
                id BIGINT NOT NULL COMMENT '用户ID',
                name STRING COMMENT '用户名',
                created_at TIMESTAMP
            )
            USING iceberg
            PARTITIONED BY (day(created_at))
        ''')

        # 插入数据
        execute_iceberg_sql("INSERT INTO my_db.users VALUES (1, 'test', current_timestamp())")

        # 查询数据
        df = execute_iceberg_sql("SELECT * FROM my_db.users")
        df.show()
    """
    catalog_name, catalog_config = _get_catalog_config(cluster)
    spark = get_spark_session(catalog_name, catalog_config)
    sql = sql.strip().rstrip(';')
    return spark.sql(sql)


def execute_iceberg_sql_batch(sql_statements, cluster=None):
    """
    批量执行多条 Iceberg SQL 语句

    参数:
        sql_statements: SQL 语句列表 或 用分号分隔的多条 SQL 字符串
        cluster: 集群名称，为 None 时使用默认 catalog

    返回:
        list[DataFrame]: 每条 SQL 的执行结果列表
    """
    if isinstance(sql_statements, str):
        # 按分号拆分（简单拆分，不处理字符串内的分号）
        sql_statements = [s.strip() for s in sql_statements.split(';') if s.strip()]

    catalog_name, catalog_config = _get_catalog_config(cluster)
    spark = get_spark_session(catalog_name, catalog_config)

    results = []
    for sql in sql_statements:
        sql = sql.strip().rstrip(';')
        results.append(spark.sql(sql))
    return results


def stop_iceberg_spark(catalog_name=None):
    """
    停止 Iceberg 使用的 SparkSession

    参数:
        catalog_name: 指定要停止的 catalog 对应的 session，为 None 时停止所有
    """
    with _lock:
        if catalog_name:
            session = _spark_sessions.pop(catalog_name, None)
            if session:
                session.stop()
        else:
            for name, session in _spark_sessions.items():
                try:
                    session.stop()
                except Exception:
                    pass
            _spark_sessions.clear()