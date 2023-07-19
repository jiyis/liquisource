FROM mirrors.tencent.com/qidian-analytics/liquibase:latest

LABEL maintainer="garypdong"

RUN pip3 install --no-cache-dir liquisource

WORKDIR /app