#!/bin/bash

basedir="$1"
njobs=$2

for i in $(seq 0 $(($njobs - 1))); do 
	mkdir -p "${basedir}/job_${i}/logs"
done
