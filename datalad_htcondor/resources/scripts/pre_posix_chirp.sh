#!/bin/bash

# pre-flight script for preparing the execution dir -- plain POSIX
# version. condor_chirp is used to obtain the input files, based
# on a list that the job supplied

set -e -u -x

printf "preflight" > status
# minimum input/output setup
mkdir stamps
mkdir dataset

# if there is no input spec we can go home early
if [ ! -f input_files ]; then
  printf "preflight_completed" > status
  touch stamps/prep_complete
  exit 0
fi

chirp_exec="$(condor_config_val LIBEXEC)/condor_chirp"

# with this preflight script we can only handle path locations
# no URLs
dspath_prefix="$(cat source_dataset_location)"

# obtain input files
while IFS= read -rd '' file; do
  mkdir -p dataset/"$(dirname ${file:${#dspath_prefix}})"
  "${chirp_exec}" fetch "${file}" dataset/"${file:${#dspath_prefix}}"
done < input_files

printf "preflight_completed" > status
touch stamps/prep_complete
