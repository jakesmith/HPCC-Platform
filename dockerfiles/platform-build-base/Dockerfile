##############################################################################
#
#    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
##############################################################################

# Build container image containing all 3rd-party tools required to build HPCC platform

FROM ubuntu:22.04 as build
ENV DEBIAN_FRONTEND=noninteractive
ARG BASE_VER

# This is version 8.12-rc2 of the platform-build-base, and anyone building/using it should have set that in BASE_VER
RUN [ "${BASE_VER}" = "8.12-rc2" ]
 
RUN apt clean -y && \
    apt autoclean -y && \
    apt install -y -f && \
    apt autoremove -y && \
    apt-get update -y

RUN apt-get install -y bison flex build-essential binutils-dev curl lsb-release libldap2-dev libcppunit-dev libicu-dev \
    libxslt1-dev zlib1g-dev libboost-regex-dev libarchive-dev python3-dev default-jdk libapr1-dev libaprutil1-dev \
    libiberty-dev libhiredis-dev libtbb-dev libxalan-c-dev libnuma-dev libevent-dev libatlas-base-dev libblas-dev \
    python3-dev default-libmysqlclient-dev libsqlite3-dev r-base-dev r-cran-rcpp r-cran-rinside r-cran-inline \
    libmemcached-dev pkg-config libtool autotools-dev automake libssl-dev git libcurl4-openssl-dev cmake libbsd-dev


RUN apt-get install -y \
  bash-completion \
  expect \
  git \
  nano \
  valgrind \
  sudo \
  vim \
  gdb \
  software-properties-common \  
  lsb-release \
  jq

RUN curl -fsSL https://deb.nodesource.com/setup_19.x | bash -
RUN apt-get install -y nodejs

WORKDIR /
RUN apt-get clean -y

FROM ubuntu:20.04
COPY --from=build / /
