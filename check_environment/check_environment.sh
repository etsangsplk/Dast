#!/bin/bash

cd `dirname ${BASH_SOURCE-$0}`
. env.sh

ping_threshold=1
for host in `cat $HOSTS`; do
  ping_lat=`ping $host -c 1| tail -1|sed -n 's/.* = .*\/\(.*\)\/.*/\1/p'`
  if [ $(echo "$ping_lat < $ping_threshold"|bc) -eq 1 ]
  then
      echo "PINGsuccess"
  else
      echo "PINGfailed"
  fi
done


cpu=`top -b -n1 | fgrep "Cpu(s)" | tail -1 | awk -F'id,' '{split($1, vs, ","); v=vs[length(vs)]; sub(/\s+/, "", v);sub(/\s+/, "", v); printf "%d", 100-v;}'`
if (($cpu<5)) 
then
    echo "CPUsuccess"
else
    echo "CPUfailed"
fi

