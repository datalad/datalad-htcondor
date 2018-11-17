#!/bin/sh
#

set -u -e -x

# fail if we cannot verify that the preflight ran in full
[ ! -f stamps/prep_complete ] && exit 100 || true

# run in root of dataset
cd dataset

touch ../stamps/job_start
printf "job_attempted" > status

# eval not exec, we want to keep this script in control
# of error management
eval "$@"

touch ../stamps/job_success
