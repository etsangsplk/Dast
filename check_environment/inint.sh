#!/bin/bash
#arg nnodes
cd `dirname ${BASH_SOURCE-$0}`
. env.sh

i=0
for host in `cat $HOSTS`; do
  ssh -oStrictHostKeyChecking=no hkucs@$host 'sudo apt-get -y install bc'
done
