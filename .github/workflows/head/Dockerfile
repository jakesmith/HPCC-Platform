ARG BASE_IMAGE
ARG HEAD_IMAGE
FROM ${BASE_IMAGE} AS regress-setup

COPY --from=${HEAD_IMAGE} --chown=hpcc:hpcc /hpcc-dev/hpccinstall /hpcc-dev/hpccinstall
COPY --from=${HEAD_IMAGE} --chown=hpcc:hpcc /hpcc-dev/HPCC-Platform/testing/regress /hpcc-dev/regress

WORKDIR /hpcc-dev/regress
COPY ecl-test.json .

RUN /bin/bash -c "source /hpcc-dev/hpccinstall/opt/HPCCSystems/sbin/hpcc_setenv && /hpcc-dev/hpccinstall/opt/HPCCSystems/etc/init.d/hpcc-init start && ./ecl-test setup"
