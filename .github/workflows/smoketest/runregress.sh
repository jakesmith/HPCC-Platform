#!/bin/bash

runtype=$1
shift
source /hpcc-dev/hpccinstall/opt/HPCCSystems/sbin/hpcc_setenv
/hpcc-dev/hpccinstall/opt/HPCCSystems/etc/init.d/hpcc-init start

./ecl-test run --pq 2 --target ${runtype} --excludeclass embedded,3rdparty,spray $@

