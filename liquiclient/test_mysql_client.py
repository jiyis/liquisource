#!/usr/local/bin/python3
# coding: utf8
import re
import sys
import unittest
from urllib.parse import urlparse, parse_qs


class Test(unittest.TestCase):
    def test_parse_jdbc_dsn(self):
        dsn = 'jdbc:mysql//mysql-tencentbeacon-mysql-master.beacon-base:3306/liquibase?chacterEncoding=assdsa&aaa=111'

        # 去除 "jdbc:" 前缀
        dsn = "mysql://" + dsn[12:]

        # 解析 DSN
        # 当成url解析
        url_obj = urlparse(dsn)
        query_params = parse_qs(url_obj.query)

        print(url_obj)
        print(query_params)

        config = {
            "host": url_obj.hostname,
            "port": url_obj.port,
            "database": url_obj.path.lstrip("/"),
            "user": 11,
            "password": 22,
            "charset": query_params.get('characterEncoding', ["utf8"])[0]
        }

        print(config)


        self.assertEqual(1, 1)


if __name__ == '__main__':
    unittest.main()
