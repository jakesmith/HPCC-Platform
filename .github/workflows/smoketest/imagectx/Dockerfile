# Prepare build environemnt, install pre-requisites
FROM ubuntu:20.04

RUN apt-get update
RUN apt-get install -y cmake
RUN apt-get install -y bison flex build-essential binutils-dev libldap2-dev libcppunit-dev libicu-dev
RUN apt-get install -y libxslt1-dev zlib1g-dev libboost-regex-dev libarchive-dev python-dev libv8-dev default-jdk
RUN apt-get install -y libapr1-dev libaprutil1-dev libiberty-dev libhiredis-dev libtbb-dev libxalan-c-dev libnuma-dev
RUN apt-get install -y nodejs libevent-dev libatlas-base-dev libblas-dev python3-dev default-libmysqlclient-dev
RUN apt-get install -y libsqlite3-dev r-base-dev r-cran-rcpp r-cran-rinside r-cran-inline libmemcached-dev
RUN apt-get install -y libcurl4-openssl-dev pkg-config libtool autotools-dev automake libssl-dev


RUN apt-get install -y wget curl

RUN curl -sL https://deb.nodesource.com/setup_15.x | bash -
RUN apt-get install -y nodejs

RUN apt-get install -y git

RUN groupadd -g 1000 hpcc
RUN useradd -s /bin/bash -r -m -N -c "hpcc runtime User" -u 999 -g hpcc hpcc
RUN passwd -l hpcc 

RUN mkdir /hpcc-dev
RUN chown hpcc:hpcc /hpcc-dev
USER hpcc
