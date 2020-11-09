ARG HEAD_IMAGE
ARG BASE_IMAGE
FROM $HEAD_IMAGE AS head
FROM $BASE_IMAGE

COPY --from=head --chown=hpcc:hpcc /hpcc-dev/hpccinstall /hpcc-dev/hpccinstall
COPY --from=head --chown=hpcc:hpcc /hpcc-dev/HPCC-Platform/testing/regress /hpcc-dev/regress

WORKDIR /hpcc-dev/regress
COPY ecl-test.json .

RUN /bin/bash -c "source /hpcc-dev/hpccinstall/opt/HPCCSystems/sbin/hpcc_setenv && /hpcc-dev/hpccinstall/opt/HPCCSystems/etc/init.d/hpcc-init start && ./ecl-test setup"
