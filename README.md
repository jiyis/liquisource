# liquibase数据源驱动
> liquibase引入python3脚本，统一管理管理mongo、clickhouse的库表结构。changelog记录还是选在记录到mysql中，这样业务上会更加灵活
```xml
<changeSet id="xxxxx" author="xxxxxx" labels="mongo">
    <comment>xxxxx</comment>
    <executeCommand executable="python3">
        <arg value="script/db_tag/creat_collection.py"/>
    </executeCommand>
</changeSet>
```
```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from liquibase_datasource import *


def create_tag_database():
    # 获取mongo链接实例
    client = get_client(filepath)


    return client[db]


def create_tag_collection():
    # 获取mongo链接实例
    db_name = get_tenant_shard(tag_database)
    client = get_client()
    db = client[db_name]

    # db开启分片
    client.admin.command("enableSharding", db_name)

    db.create_collection(tag_collection)

    # 创建索引
    coll = db[tag_collection]
    coll.create_index(
        [("id", 1), ("name", 1)])


if __name__ == "__main__":
    # 创建标签集合
    create_tag_database()[run.py](..%2F..%2F..%2FDownloads%2Frun.py)
    create_tag_collection()

```

## Iceberg 建表

通过 **PySpark** + REST Catalog（如 Polaris）执行 Iceberg SQL 语句，支持完整的 Spark SQL 语法和多种存储后端。

### 前置要求

- **Java 8/11/17**（PySpark 运行需要 JVM 环境）
- Python 3.8+

```bash
# 检查 Java 版本
java -version

# 安装依赖
pip install -r requirements.txt
```

### 配置说明

在 `liquibase.properties` 中添加 Iceberg 相关配置：

**基础配置（必填）：**
```properties
iceberg.catalog.name=my_catalog
iceberg.catalog.type=rest
iceberg.catalog.uri=http://your-polaris-server:8080/api/catalog
iceberg.catalog.warehouse=my_catalog
```

**REST Catalog 认证（Polaris 等需要 OAuth2 认证的 Catalog）：**
```properties
iceberg.catalog.credential=root:s3cr3t
iceberg.catalog.scope=PRINCIPAL_ROLE:ALL
```

**存储配置 — 腾讯云 COS：**
```properties
iceberg.s3.type=cos
iceberg.s3.endpoint=cos.ap-guangzhou.myqcloud.com
iceberg.s3.access_key=your-secret-id
iceberg.s3.secret_key=your-secret-key
```

**存储配置 — AWS S3：**
```properties
iceberg.s3.type=s3
iceberg.s3.endpoint=s3.amazonaws.com
iceberg.s3.access_key=your-access-key
iceberg.s3.secret_key=your-secret-key
iceberg.s3.region=us-east-1
```

**存储配置 — MinIO：**
```properties
iceberg.s3.type=minio
iceberg.s3.endpoint=http://minio:9000
iceberg.s3.access_key=minioadmin
iceberg.s3.secret_key=minioadmin
```

**存储配置 — 阿里云 OSS：**
```properties
iceberg.s3.type=oss
iceberg.s3.endpoint=oss-cn-hangzhou.aliyuncs.com
iceberg.s3.access_key=your-access-key
iceberg.s3.secret_key=your-secret-key
```

**集群模式** 配置前缀改为 `{cluster}.iceberg.*`，例如：
```properties
cluster1.iceberg.catalog.name=my_catalog
cluster1.iceberg.s3.type=cos
cluster1.iceberg.s3.endpoint=cos.ap-guangzhou.myqcloud.com
...
```

### 代码示例

#### 方式一：Python API 建表

```python
from liquiclient.iceberg_client import get_iceberg_client
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, StringType, NestedField

# 获取 pyiceberg catalog 实例
catalog = get_iceberg_client()

# 列出 namespaces
namespaces = catalog.list_namespaces()

# 创建 namespace
catalog.create_namespace_if_not_exists("my_db")

# 建表
schema = Schema(
    NestedField(field_id=1, name="id", field_type=LongType(), required=True),
    NestedField(field_id=2, name="name", field_type=StringType(), required=False),
)
table = catalog.create_table("my_db.my_table", schema=schema)
```

#### 方式二：Spark SQL 建表（推荐）

基于 PySpark，支持完整的 Spark SQL Iceberg 语法，包括 DDL、DML、查询等：

```python
from liquiclient.iceberg_client import execute_iceberg_sql, stop_iceberg_spark

# 创建 Iceberg 表
execute_iceberg_sql("""
    CREATE TABLE IF NOT EXISTS my_db.users (
        id BIGINT NOT NULL COMMENT '用户ID',
        name STRING COMMENT '用户名',
        age INT,
        created_at TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (day(created_at))
""")

# 带分区 + 表属性
execute_iceberg_sql("""
    CREATE TABLE my_db.events (
        id BIGINT NOT NULL,
        event_type STRING,
        amount DECIMAL(18, 2),
        event_time TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (day(event_time), bucket(16, id))
    TBLPROPERTIES ('write.format.default' = 'parquet')
""")

# 插入数据
execute_iceberg_sql("INSERT INTO my_db.users VALUES (1, 'test', 25, current_timestamp())")

# 查询数据
df = execute_iceberg_sql("SELECT * FROM my_db.users")
df.show()

# ALTER TABLE
execute_iceberg_sql("ALTER TABLE my_db.users ADD COLUMN email STRING COMMENT '邮箱'")

# 删除表
execute_iceberg_sql("DROP TABLE IF EXISTS my_db.users")

# 创建 namespace
execute_iceberg_sql("CREATE NAMESPACE IF NOT EXISTS my_db")

# 集群模式
execute_iceberg_sql("CREATE TABLE my_db.t1 (id BIGINT) USING iceberg", cluster="cluster1")

# 批量执行
from liquiclient.iceberg_client import execute_iceberg_sql_batch
execute_iceberg_sql_batch("""
    CREATE NAMESPACE IF NOT EXISTS my_db;
    CREATE TABLE IF NOT EXISTS my_db.t1 (id BIGINT, name STRING) USING iceberg;
    INSERT INTO my_db.t1 VALUES (1, 'hello')
""")

# 使用完毕后停止 SparkSession
stop_iceberg_spark()
```

**支持的 SQL 语法（Spark SQL 完整语法）：**

| 功能 | 语法示例 |
|------|---------|
| 建表 | `CREATE TABLE ... USING iceberg` |
| 删表 | `DROP TABLE IF EXISTS ...` |
| 改表 | `ALTER TABLE ... ADD/DROP/RENAME COLUMN` |
| 插入 | `INSERT INTO ...`, `INSERT OVERWRITE ...` |
| 查询 | `SELECT ...`, `MERGE INTO ...` |
| 分区 | `PARTITIONED BY (col)`, `PARTITIONED BY (year(ts), bucket(16, id))` |
| 表属性 | `TBLPROPERTIES ('key' = 'value')` |
| Namespace | `CREATE/DROP NAMESPACE/SCHEMA/DATABASE` |
| 快照 | `SELECT * FROM my_table.snapshots` |
| 时间旅行 | `SELECT * FROM my_table TIMESTAMP AS OF '2024-01-01'` |

### 验证脚本

项目提供了 `test_iceberg.py` 验证脚本，可用于快速验证连接和建表：

```bash
# 1. 编辑 test_iceberg.py 顶部的配置区域，填入你的实际信息
# 2. 运行验证
python test_iceberg.py
```

## 发布包
```python
python3 -m pip install --upgrade build
python3 -m build
python3 -m pip install --upgrade twine
python3 -m twine upload  dist/*

```