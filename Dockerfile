FROM mirrors.tencent.com/tlinux/tlinux3.2-minimal:latest

LABEL maintainer="jiyis"

ENV JAVA_HOME=/usr/local/apache-maven-3.9.6-bin
ENV PATH="${PATH}:/usr/local/apache-maven-3.9.6-bin/bin"

RUN mkdir /app \
    && rm -rf /etc/yum.repos.d/* \
    && rpm -ivh --force http://mirrors.tencent.com/tlinux/3.1/Updates/x86_64/RPMS/tencentos-release-3.1-11.tl3.x86_64.rpm \
    && rpm -ivh --force http://mirrors.tencent.com/tlinux/3.1/Updates/x86_64/RPMS/epel-release-8-20.tl3.1.noarch.rpm \
    && yum -y install wget \
    && yum -y install tar \
    && yum -y install gzip \
    && yum -y install python38 \
    && yum -y install java-8-konajdk \
    && pip3 install --upgrade pip \
    && pip3 install --no-cache-dir liquisource \
    && wget https://dlcdn.apache.org/maven/maven-3/3.9.6/binaries/apache-maven-3.9.6-bin.tar.gz \
    && tar -xzvf apache-maven-3.9.6-bin.tar.gz -C /usr/local \
    && yum clean all && rm -rf /var/cache/yum


WORKDIR /app

ENTRYPOINT ["mvn"]

CMD ["pip3"]