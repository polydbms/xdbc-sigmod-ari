FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        gcc \
        g++ \
        libc-dev \
        make \
        cmake \
        ca-certificates \
        git \
        libssl-dev \
        alien \
        curl \
        nano \
        openjdk-8-jdk \
        gpg  \
        gnupg2  \
        software-properties-common  \
        apt-transport-https  \
        lsb-release  \
        mysql-client \
        libmysqlclient-dev \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc| gpg --dearmor -o /etc/apt/trusted.gpg.d/postgresql.gpg
RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ `lsb_release -cs`-pgdg main" | tee  /etc/apt/sources.list.d/pgdg.list

RUN apt-get update &&  apt-get install -y --no-install-recommends \
                              postgresql-13 \
                              postgresql-server-dev-13 \
                              odbc-postgresql \
                              unixodbc-dev

ARG CHECK_COMMAND="arch"

RUN if $CHECK_COMMAND | grep -q "aarch64"; then \
        ln -s /usr/lib/jvm/java-8-openjdk-arm64/jre/lib/aarch64/server/libjvm.so /usr/lib/libjvm.so; \
    else \
        ln -s /usr/lib/jvm/java-8-openjdk-amd64/jre/lib/amd64/server/libjvm.so /usr/lib/libjvm.so; \
    fi


RUN git clone https://github.com/pgspider/jdbc_fdw.git \
    && cd jdbc_fdw \
    && make clean  USE_PGXS=1 && make install  USE_PGXS=1


USER postgres

RUN    /etc/init.d/postgresql start &&\
    psql --command "ALTER USER postgres WITH PASSWORD '123456';" &&\
    createdb -O postgres db1

RUN echo "host all  all    0.0.0.0/0  md5" >> /etc/postgresql/13/main/pg_hba.conf

RUN echo "listen_addresses='*'" >> /etc/postgresql/13/main/postgresql.conf
EXPOSE 5432

VOLUME  ["/etc/postgresql", "/var/log/postgresql", "/var/lib/postgresql"]

CMD ["/usr/lib/postgresql/13/bin/postgres", "-D", "/var/lib/postgresql/13/main", "-c", "config_file=/etc/postgresql/13/main/postgresql.conf"]