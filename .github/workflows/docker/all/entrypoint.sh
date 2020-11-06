#!/bin/bash

/opt/HPCCSystems/etc/init.d/hpcc-init start

source /opt/HPCCSystems/sbin/hpcc_setenv
cd /hpcc-dev/HPCC-Platform/testing/regress

./ecl-test setup
./ecl-test query --target thor --pq 2 --excludeclass embedded,3rdparty,spray sqagg*.ecl
