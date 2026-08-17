FROM apache/airflow:2.9.0

USER root

RUN curl -fsSL https://download.docker.com/linux/static/stable/x86_64/docker-25.0.3.tgz -o docker.tgz && \
    tar xzvf docker.tgz && \
    mv docker/docker /usr/local/bin/docker && \
    rm -rf docker docker.tgz

USER airflow