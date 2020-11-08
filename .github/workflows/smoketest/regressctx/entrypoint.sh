#!/bin/bash

runtype=$1
source /hpcc-dev/hpccinstall/opt/HPCCSystems/sbin/hpcc_setenv
/hpcc-dev/hpccinstall/opt/HPCCSystems/etc/init.d/hpcc-init start

case ${runtype} in

  setup)
    ./ecl-test setup
    tar cvpfz state.tgz /hpcc-dev/install

    ;;

  *)
    ./ecl-test run --pq 2 --target ${runtype} --excludeclass embedded,3rdparty,spray
    ;;
esac
    

