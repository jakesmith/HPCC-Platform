# STAGE - build image with pre-requisites
FROM ubuntu:20.04 AS base-image

RUN apt-get update

# JCSMORE - would be interesting to retrieve pre-requisites, based on target platform, automatically
# e.g. have wiki definitions formalized/parsable and fetch them from there

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
WORKDIR /hpcc-dev
RUN chown hpcc:hpcc /hpcc-dev
USER hpcc


# STAGE - build master base
FROM base-image AS major-minor-base-build

ARG BASE_REPO=hpcc-systems
ARG MAJOR_MINOR_BASE=community_7.0.0
ARG BUILD_THREADS
ARG BUILD_TYPE=RelWithDebInfo
ARG USE_CPPUNIT=0

RUN if [ -n "${BUILD_THREADS}" ] ; then echo ${BUILD_THREADS} > ~/build_threads; else echo $(nproc) > ~/build_threads ; fi
RUN echo Building with $(cat ~/build_threads) threads
RUN git clone https://github.com/${BASE_REPO}/HPCC-Platform.git
WORKDIR /hpcc-dev/HPCC-Platform
RUN git checkout ${MAJOR_MINOR_BASE}
RUN git submodule update --init --recursive

RUN mkdir /hpcc-dev/hpccinstall
RUN mkdir /hpcc-dev/build
WORKDIR /hpcc-dev/build
RUN cmake /hpcc-dev/HPCC-Platform -Wno-dev -DDESTDIR=/hpcc-dev/hpccinstall -DINCLUDE_PLUGINS=0 -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DUSE_PYTHON2=0 -DUSE_PYTHON3=0 -DSUPPRESS_SPARK=1 -DUSE_CPPUNIT=${USE_CPPUNIT} -DUSE_CASSANDRA=Off -DSUPPRESS_BUILD_CONFIG_H=On
RUN make -j$(cat ~/build_threads) jlib
#RUN make -j$(cat ~/build_threads) esp
#RUN make -j$(cat ~/build_threads) roxie
#RUN make -j$(cat ~/build_threads) ws_workunits ecl
#RUN make -j$(cat ~/build_threads) thormaster_lcr thorslave_lcr
#RUN make -j$(cat ~/build_threads) eclccserver hthor agentexec
#RUN make -j$(cat ~/build_threads)
RUN echo =====================================
RUN echo === Building major-minor-base-build =
RUN echo =====================================



# STAGE - build base branch
FROM major-minor-base-build AS base-branch-build

ARG BASE_BRANCH=master
ARG BASE_SHA=sha # NB: not used, but set to dirty cache if different
WORKDIR /hpcc-dev/HPCC-Platform
RUN git checkout ${BASE_BRANCH}
RUN git submodule update --init --recursive

WORKDIR /hpcc-dev/build
RUN cmake /hpcc-dev/HPCC-Platform -Wno-dev -DDESTDIR=/hpcc-dev/hpccinstall -DINCLUDE_PLUGINS=0 -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DUSE_PYTHON2=0 -DUSE_PYTHON3=0 -DSUPPRESS_SPARK=1 -DUSE_CPPUNIT=${USE_CPPUNIT} -DUSE_CASSANDRA=Off -DSUPPRESS_BUILD_CONFIG_H=On -DSUPPRESS_BUILD_CONFIG_H=On
RUN make --debug=a -j$(cat ~/build_threads) jlib
#RUN make -j$(cat ~/build_threads) esp
#RUN make -j$(cat ~/build_threads) roxie
#RUN make -j$(cat ~/build_threads) ws_workunits ecl
#RUN make -j$(cat ~/build_threads) thormaster_lcr thorslave_lcr
#RUN make -j$(cat ~/build_threads) eclccserver hthor agentexec
#RUN make -j$(cat ~/build_threads)
RUN echo =====================================
RUN echo === Building base-branch-build ======
RUN echo =====================================


# STAGE - build head branch
FROM base-branch-build AS head-branch-build

ARG HEAD_BRANCH=master
ARG HEAD_REPO=hpcc-systems
ARG HEAD_SHA=sha # NB: not used, but set to dirty cache if different
WORKDIR /hpcc-dev/HPCC-Platform
RUN git fetch --depth=1 https://github.com/${HEAD_REPO}/HPCC-Platform ${HEAD_BRANCH}:${HEAD_BRANCH}
RUN git checkout ${HEAD_BRANCH}
RUN git submodule update --init --recursive

WORKDIR /hpcc-dev/build
RUN cmake /hpcc-dev/HPCC-Platform -Wno-dev -DDESTDIR=/hpcc-dev/hpccinstall -DINCLUDE_PLUGINS=0 -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DUSE_PYTHON2=0 -DUSE_PYTHON3=0 -DSUPPRESS_SPARK=1 -DUSE_CPPUNIT=${USE_CPPUNIT} -DUSE_CASSANDRA=Off -DSUPPRESS_BUILD_CONFIG_H=On -DSUPPRESS_BUILD_CONFIG_H=On
RUN make --debug=a -j$(cat ~/build_threads) jlib
#RUN make -j$(cat ~/build_threads) esp
#RUN make -j$(cat ~/build_threads) roxie
#RUN make -j$(cat ~/build_threads) ws_workunits ecl
#RUN make -j$(cat ~/build_threads) thormaster_lcr thorslave_lcr
#RUN make -j$(cat ~/build_threads) eclccserver hthor agentexec
#RUN make -j$(cat ~/build_threads)
#RUN make install
RUN echo =====================================
RUN echo === Building head-branch-build ======
RUN echo =====================================

# For debugging image only
USER root
RUN apt-get install -y sudo
RUN usermod -aG sudo hpcc
RUN echo "hpcc     ALL=(ALL) NOPASSWD:ALL"  >> /etc/sudoers
USER hpcc


# STAGE - Regress setup
FROM base-image AS regress-setup

COPY --from=head-branch-build --chown=hpcc:hpcc /hpcc-dev/hpccinstall /hpcc-dev/hpccinstall
COPY --from=head-branch-build --chown=hpcc:hpcc /hpcc-dev/HPCC-Platform/testing/regress /hpcc-dev/regress

WORKDIR /hpcc-dev/regress
COPY ecl-test.json .

#RUN /bin/bash -c "source /hpcc-dev/hpccinstall/opt/HPCCSystems/sbin/hpcc_setenv && /hpcc-dev/hpccinstall/opt/HPCCSystems/etc/init.d/hpcc-init start && ./ecl-test setup"
RUN echo =====================================
RUN echo === In regress-setup ================
RUN echo =====================================
