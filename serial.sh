#!/bin/bash

for filename in log/proc-S*.log; do 
  cat $filename | grep "Going" | cut -d '=' -f2 > "log/$(basename "$filename" .log).serial"
done 
