# STAGE - build image with pre-requisites
FROM ubuntu:20.04 AS base-image

SHELL ["/bin/bash", "-c"]
ARG DEBIAN_FRONTEND=noninteractive # Not using ENV, so not part of runtime image
RUN TZ=Europe/London \
    apt-get update

# JCSMORE - would be interesting to retrieve pre-requisites, based on target platform, automatically
# e.g. have wiki definitions formalized/parsable and fetch them from there

RUN apt-get install -y cmake bison flex build-essential binutils-dev libldap2-dev libcppunit-dev libicu-dev libxslt1-dev zlib1g-dev libboost-regex-dev libarchive-dev python-dev libv8-dev default-jdk libapr1-dev libaprutil1-dev libiberty-dev libhiredis-dev libtbb-dev libxalan-c-dev libnuma-dev nodejs libevent-dev libatlas-base-dev libblas-dev python3-dev default-libmysqlclient-dev libsqlite3-dev r-base-dev r-cran-rcpp r-cran-rinside r-cran-inline libmemcached-dev libcurl4-openssl-dev pkg-config libtool autotools-dev automake libssl-dev wget curl git

RUN curl -sL https://deb.nodesource.com/setup_15.x | bash - && apt-get install -y nodejs

RUN groupadd -g 1000 hpcc && \
    useradd -s /bin/bash -r -m -N -c "hpcc runtime User" -u 999 -g hpcc hpcc && \
    passwd -l hpcc && \
    mkdir -p /hpcc-dev/build && \
    mkdir -p /hpcc-dev/hpccinstall && \
    chown -R hpcc:hpcc /hpcc-dev 

USER hpcc
WORKDIR /hpcc-dev


# STAGE - build master base
FROM base-image AS major-minor-base-build

ARG BASE_REPO=hpcc-systems
ARG MAJOR_MINOR_BASE=community_7.0.0
ARG BUILD_THREADS
ARG BUILD_TYPE=RelWithDebInfo
ARG USE_CPPUNIT=0

RUN git clone https://github.com/${BASE_REPO}/HPCC-Platform.git
RUN cd HPCC-Platform && \
    git checkout ${MAJOR_MINOR_BASE} && \
    git submodule update --init --recursive && \
    cd /hpcc-dev/build && \
    cmake /hpcc-dev/HPCC-Platform -Wno-dev -DDESTDIR=/hpcc-dev/hpccinstall -DINCLUDE_PLUGINS=0 -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DUSE_PYTHON2=0 -DUSE_PYTHON3=0 -DSUPPRESS_SPARK=1 -DUSE_CPPUNIT=${USE_CPPUNIT} -DUSE_CASSANDRA=Off -DSUPPRESS_BUILD_CONFIG_H=On -DCMAKE_CXX_FLAGS="-DBUILD_TAG=\\\"dummytag\\\" -DBUILD_VERSION_MAJOR=1 -DBUILD_VERSION_MINOR=1 -DBUILD_VERSION_POINT=0"

RUN cd /hpcc-dev/build && \
    if grep -q '^lz4:' Makefile; then make -j$(nproc) lz4; fi && \
    if grep -q '^yaml:' Makefile; then make -j$(nproc) yaml; fi && \
    if grep -q '^aws\-sdk\-cpp:' Makefile; then make -j$(nproc) aws-sdk-cpp; fi && \
    if grep -q '^azure\-storage\-cpplite:' Makefile; then make -j$(nproc) azure-storage-cpplite; fi && \
    if grep -q '^libbase58:' Makefile; then make -j$(nproc) libbase58; fi && \
    if grep -q '^tbb:' Makefile; then make -j$(nproc) tbb; fi && \
    if grep -q '^aeron_client:' Makefile; then make -j$(nproc) aeron_client; fi && \
    if grep -q '^aeron_driver:' Makefile; then make -j$(nproc) aeron_driver; fi && \
    if grep -q '^antlr3c:' Makefile; then make -j$(nproc) antlr3c; fi


# STAGE - build base branch
FROM major-minor-base-build AS base-branch-build

ARG BASE_BRANCH=master
ARG BASE_SHA=dummysha # NB: not used, but set to dirty cache if different
RUN cd /hpcc-dev/HPCC-Platform && \
    git checkout ${BASE_BRANCH} && \
    git submodule update --init --recursive && \
    cd /hpcc-dev/build && \
    cmake /hpcc-dev/HPCC-Platform -Wno-dev -DDESTDIR=/hpcc-dev/hpccinstall -DINCLUDE_PLUGINS=0 -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DUSE_PYTHON2=0 -DUSE_PYTHON3=0 -DSUPPRESS_SPARK=1 -DUSE_CPPUNIT=${USE_CPPUNIT} -DUSE_CASSANDRA=Off -DSUPPRESS_BUILD_CONFIG_H=On -DCMAKE_CXX_FLAGS="-DBUILD_TAG=\\\"dummytag\\\" -DBUILD_VERSION_MAJOR=1 -DBUILD_VERSION_MINOR=1 -DBUILD_VERSION_POINT=0"

RUN make -j$(nproc)


# STAGE - build head branch
FROM base-branch-build AS head-branch-build

ARG HEAD_BRANCH=master
ARG HEAD_REPO=hpcc-systems
ARG HEAD_SHA=dummysha # NB: not used, but set to dirty cache if different
RUN cd /hpcc-dev/HPCC-Platform && \
    git fetch --depth=1 https://github.com/${HEAD_REPO}/HPCC-Platform ${HEAD_BRANCH}:${HEAD_BRANCH} && \
    git checkout ${HEAD_BRANCH} && \
    git submodule update --init --recursive && \
    cd /hpcc-dev/build && \
    cmake /hpcc-dev/HPCC-Platform -Wno-dev -DDESTDIR=/hpcc-dev/hpccinstall -DINCLUDE_PLUGINS=0 -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DUSE_PYTHON2=0 -DUSE_PYTHON3=0 -DSUPPRESS_SPARK=1 -DUSE_CPPUNIT=${USE_CPPUNIT} -DUSE_CASSANDRA=Off -DSUPPRESS_BUILD_CONFIG_H=On -DCMAKE_CXX_FLAGS="-DBUILD_TAG=\\\"dummytag\\\" -DBUILD_VERSION_MAJOR=1 -DBUILD_VERSION_MINOR=1 -DBUILD_VERSION_POINT=0"

RUN cd /hpcc-dev/build && \
    make -j$(nproc) install

# For debugging image only
#USER root
#RUN apt-get install -y sudo
#RUN usermod -aG sudo hpcc
#RUN echo "hpcc     ALL=(ALL) NOPASSWD:ALL"  >> /etc/sudoers
#USER hpcc


# STAGE - Regress setup
FROM base-image AS regress-setup

COPY --from=head-branch-build --chown=hpcc:hpcc /hpcc-dev/hpccinstall /hpcc-dev/hpccinstall
COPY --from=head-branch-build --chown=hpcc:hpcc /hpcc-dev/HPCC-Platform/testing/regress /hpcc-dev/regress
COPY --chown=hpcc:hpcc runregress.sh .

RUN /bin/bash -c "source /hpcc-dev/hpccinstall/opt/HPCCSystems/sbin/hpcc_setenv && /hpcc-dev/hpccinstall/opt/HPCCSystems/etc/init.d/hpcc-init start && cd /hpcc-dev/regress && ./ecl-test setup"

RUN chmod +x ./runregress.sh
ENTRYPOINT ["/hpcc-dev/regress/runregress.sh"]
