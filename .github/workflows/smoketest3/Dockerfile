# STAGE - build image with pre-requisites
FROM ubuntu:20.04 AS base-image

SHELL ["/bin/bash", "-c"]
ARG DEBIAN_FRONTEND=noninteractive
RUN TZ=Europe/London \
    apt-get update

# JCSMORE - would be interesting to retrieve pre-requisites, based on target platform, automatically
# e.g. have wiki definitions formalized/parsable and fetch them from there

RUN echo in build of base-image

USER hpcc
WORKDIR /hpcc-dev/build


# STAGE - build master base
FROM base-image AS major-minor-base-build

ARG BASE_REPO=hpcc-systems
ARG MAJOR_MINOR_BASE=community_7.0.0
ARG BUILD_TYPE=RelWithDebInfo
ARG USE_CPPUNIT=0

RUN echo in build of major-minor-base-build


# STAGE - build base branch
FROM major-minor-base-build AS base-branch-build

ARG BASE_BRANCH=master
ARG BASE_SHA=dummysha

RUN echo in build of base-branch-build


# STAGE - build head branch
FROM base-branch-build AS head-branch-build

ARG HEAD_REPO=hpcc-systems
ARG HEAD_BRANCH=master
ARG HEAD_SHA=dummysha

RUN echo in build of head-branch-build


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
COPY --chown=hpcc:hpcc runregress.sh /hpcc-dev/regress/runregress.sh

RUN echo in build of regress-setup

ENTRYPOINT ["/hpcc-dev/regress/runregress.sh"]
