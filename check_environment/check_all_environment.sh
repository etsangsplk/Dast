#!/bin/bash
#arg nnodes

cd `dirname ${BASH_SOURCE-$0}`
. env.sh

for host in `cat $HOSTS`; do
  res=$(ssh -oStrictHostKeyChecking=no hkucs@$host $SCRIPT_HOME/check_environment.sh)
  echo $res
  PINGres=$(expr match "$res" ".*PINGfailed.*")
  CPUres=$(expr match "$res" ".*CPUfailed.*")
  if (($PINGres!=0)) && (($CPUres!=0))
  then
    echo "$host:PING check failed; CPU check failed"
  elif (($CPUres!=0))
  then
    echo "$host:PING check succeed; CPU check failed"
  elif (($PINGres!=0))
  then
    echo "$host:PING check failed; CPU check succeed"
  else
  echo "$host:check succeed"
  fi
done
