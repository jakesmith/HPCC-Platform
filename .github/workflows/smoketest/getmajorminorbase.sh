#!/bin/bash

starttag=$1 # e.g. master or candidate-7.12.x

# Aim is to find 1st tagged version of the input Major.Minor, e.g. community_7.12.0
# If $starttag is master, will chose most recent Major.Minor

if [ "$starttag" = "master" ]; then
  result=$(git -c 'versionsort.suffix=-' ls-remote --exit-code --refs --sort='version:refname' --tags https://github.com/hpcc-systems/HPCC-Platform 'community_*.*.*' | tail -n 1 | sed -e 's/^.*\(community_.*\)$/\1/')
else
  # We don't have the branch yet, only the base_ref (e.g. candidate-7.12.x)
  # If we had the head repo, we could do: git describe --abbrev=0 --tags

  regex="candidate-[0-9]+\.[0-9]+\..*"
  if [[ $starttag =~ $regex ]]; then
    findstart=$(echo $starttag | sed -e 's/candidate-\([0-9]\+\)\.\([0-9]\+\)\..*$/\1 \2/' | awk '{ print $1*100 + $2 + 0.999 }')
    result=$(git -c 'versionsort.suffix=-' ls-remote --exit-code --refs --sort='version:refname' --tags https://github.com/hpcc-systems/HPCC-Platform 'community_*.*.*' | sed -e 's/^.*\(community_\)\([0-9]\+\)\.\([0-9]\+\)\.\([0-9]\+\)-\(.*\)$/\1 \2 \3 \4 \5/' | awk '{ if ($2*100+$3+$4/100 <= FINDSTART) { print "community_" $2 "." $3 "." $4 "-" $5 } }' FINDSTART=$findstart | tail -n 1)
  fi
fi

echo ${result}
