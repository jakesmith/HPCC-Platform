ARG IMAGE_BASE
FROM ${IMAGE_BASE}

WORKDIR /hpcc-dev
ARG GITHUB_REPO_OWNER=hpcc-systems
ARG BASE_BRANCH=master
RUN git clone https://github.com/${GITHUB_REPO_OWNER}/HPCC-Platform.git && \
    cd HPCC-Platform && \
    git checkout ${BASE_BRANCH} && \
    git submodule update --init --recursive

ARG BUILD_THREADS
RUN if [ -n "${BUILD_THREADS}" ] ; then echo ${BUILD_THREADS} > ~/build_threads; else echo $(nproc) > ~/build_threads ; fi
RUN echo Building with $(cat ~/build_threads) threads

RUN mkdir /hpcc-dev/build
WORKDIR /hpcc-dev/build

ARG BUILD_TYPE=RelWithDebInfo
ARG USE_CPPUNIT=0

#RUN cmake /hpcc-dev/HPCC-Platform -Wno-dev -DINCLUDE_PLUGINS=0 -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DUSE_PYTHON2=0 -DUSE_PYTHON3=0 -DSUPPRESS_SPARK=1 -DUSE_CPPUNIT=${USE_CPPUNIT} -DUSE_CASSANDRA=Off
RUN mkdir /hpcc-dev/hpccinstall
RUN cmake /hpcc-dev/HPCC-Platform -Wno-dev -DDESTDIR=/hpcc-dev/hpccinstall -DINCLUDE_PLUGINS=0 -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DUSE_PYTHON2=0 -DUSE_PYTHON3=0 -DSUPPRESS_SPARK=1 -DUSE_CPPUNIT=${USE_CPPUNIT} -DUSE_CASSANDRA=Off

RUN make -j$(cat ~/build_threads) jlib
RUN make -j$(cat ~/build_threads) esp
RUN make -j$(cat ~/build_threads) roxie
RUN make -j$(cat ~/build_threads) ws_workunits ecl
RUN make -j$(cat ~/build_threads) thormaster_lcr thorslave_lcr
RUN make -j$(cat ~/build_threads) eclccserver hthor agentexec
RUN make -j$(cat ~/build_threads)
RUN make install
