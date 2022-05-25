FROM python:3.7-slim
MAINTAINER Inho Kim, ihnokim58@gmail.com

# ARG http_proxy=http://x.x.x.x:x
# ARG https_proxy=http://x.x.x.x:x
# COPY ca.crt /usr/local/share/ca-certificates/
# RUN update-ca-certificates

RUN apt-get update && apt-get -y upgrade && apt-get -y dist-upgrade && apt-get install -y alien libaio1 libsnappy-dev wget vim
RUN wget https://yum.oracle.com/repo/OracleLinux/OL7/oracle/instantclient/x86_64/getPackage/oracle-instantclient19.3-basiclite-19.3.0.0.0-1.x86_64.rpm --no-check-certificate
RUN alien -i --scripts oracle-instantclient*.rpm
RUN rm -f oracle-instantclient19.3*.rpm && apt-get -y autoremove && apt-get -y clean

RUN mkdir -p /opt/codepack
WORKDIR /opt/codepack

RUN mkdir -p config
RUN mkdir -p logs
RUN mkdir -p scripts
RUN mkdir -p jupyter/notebook
RUN mkdir -p app

COPY entry_point.sh .
COPY extra-requirements.txt .
COPY config config
COPY app app

RUN chmod 755 entry_point.sh

ENV CODEPACK_CONFIG_DIR /opt/codepack/config
ENV CODEPACK_CONFIG_PATH codepack.ini
ENV CODEPACK__LOGGER__LOG_DIR /opt/codepack/logs

RUN python -m pip install --upgrade pip --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org
RUN pip install --upgrade --trusted-host pypi.python.org --trusted-host pypi.org --trusted-host files.pythonhosted.org -r extra-requirements.txt

ENTRYPOINT ["/opt/codepack/entry_point.sh"]
