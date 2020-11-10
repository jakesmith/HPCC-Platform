#!/bin/bash

starttag=$1 # e.g. master or candidate-7.12.x

# Aim is to find 1st tagged version of the input Major.Minor, e.g. community_7.12.0
# If $starttag is master, will chose most recent Major.Minor

if [ "$starttag" = "master" ]; then
  result=$(git -c 'versionsort.suffix=-' ls-remote --exit-code --refs --sort='version:refname' --tags https://github.com/hpcc-systems/HPCC-Platform 'community_*.*.*' | egrep -e '\.0-1$' | tail -n 1 | sed -e 's/^.*\(community_.*\)$/\1/')
  echo $?
else
  regex="candidate-[0-9]+\.[0-9]+\..*"
  if [[ $starttag =~ $regex ]]; then
    result=$(echo $starttag | sed -e 's/candidate-\([0-9]\+\.[0-9]\+\)\..*$/community_\1.0-1/')
  fi
fi

echo ${result}
