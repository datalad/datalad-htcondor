#!/bin/bash

# pre-flight script for preparing the execution dir -- plain POSIX
# version. condor_chirp is used to obtain the input files, based
# on a list that the job supplied

set -e -u

# minimum input/output setup
mkdir stamps
mkdir dataset

# if there is no input spec we can go home early
[ ! -f input_files ] && exit 0

chirp_exec="$(condor_config_val LIBEXEC)/condor_chirp"

dspath_prefix="$(cat dataset_path)"

# obtain input files
while IFS= read -rd '' file; do
  mkdir -p dataset/"$(dirname ${file:${#dspath_prefix}})"
  "${chirp_exec}" fetch "${file}" dataset/"${file:${#dspath_prefix}}"
done < input_files

echo 'DONE -- FINAL STATE' >> stamps/PRE_stamp
ls -Rla >> stamps/PRE_stamp

touch stamps/prep_complete
