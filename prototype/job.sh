#!/bin/bash

set -e -u

echo "Main job start"

# get some info
echo $1 >> /srv/JOB
readlink -f . >> /srv/JOB
ls -la >> /srv/JOB

#tree >> JOB
#git -C dataset status >> JOB
#git -C dataset annex whereis >> JOB
#
## do work
#echo "work" > dataset/work
#git -C dataset annex add work
#git -C dataset commit -m "Work!"
#
#cat localfile >> JOB

echo "$(which fslhd)" >> /srv/JOB

echo "Main job end"
