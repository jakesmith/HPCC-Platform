ARG IMAGE_BASE
FROM ${IMAGE_BASE}

ARG BUILD_BRANCH=master
ARG GITHUB_ACTOR=hpcc-systems
WORKDIR /hpcc-dev/HPCC-Platform
RUN git fetch --depth=1 https://github.com/${GITHUB_ACTOR}/HPCC-Platform ${BUILD_BRANCH}:${BUILD_BRANCH}
RUN git checkout ${BUILD_BRANCH}
RUN git submodule update --init --recursive

WORKDIR /hpcc-dev/build

ARG BUILD_TYPE=RelWithDebInfo
ARG USE_CPPUNIT=0

#RUN cmake /hpcc-dev/HPCC-Platform -Wno-dev -DINCLUDE_PLUGINS=0 -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DUSE_PYTHON2=0 -DUSE_PYTHON3=0 -DSUPPRESS_SPARK=1 -DUSE_CPPUNIT=${USE_CPPUNIT} -DUSE_CASSANDRA=Off

RUN make -j$(cat ~/build_threads) jlib
RUN make -j$(cat ~/build_threads) esp
RUN make -j$(cat ~/build_threads) roxie
RUN make -j$(cat ~/build_threads) ws_workunits ecl
RUN make -j$(cat ~/build_threads) thormaster_lcr thorslave_lcr
RUN make -j$(cat ~/build_threads) eclccserver hthor agentexec
RUN make -j$(cat ~/build_threads)
RUN make install

# For debugging image only
USER root
RUN apt-get install -y sudo
RUN usermod -aG sudo hpcc
RUN echo "hpcc     ALL=(ALL) NOPASSWD:ALL"  >> /etc/sudoers
USER hpcc

COPY --chown=hpcc:hpcc entrypoint.sh /hpcc-dev/
RUN ls -lt /hpcc-dev/
RUN chmod +x /hpcc-dev/entrypoint.sh
RUN ls -lt /hpcc-dev/

WORKDIR /hpcc-dev/HPCC-Platform/testing/regress
COPY ./ecl-test.json .

ENTRYPOINT ["/hpcc-dev/entrypoint.sh"]
