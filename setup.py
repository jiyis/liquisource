from setuptools import setup, find_packages

setup(
    name="liquisource",
    version="0.0.1",
    packages=find_packages(),
    install_requires=[
        "setuptools",
        "jproperties",
        "clickhouse_connect",
        "pymongo"
    ],
    author="garypdong",
    author_email="garypdong@tencent.com",
    description="A package for liquibase monogo or clickhosue client.",
    keywords="liquisource",
    url="http://github.com/jiyis/liquisource"
)