#!/bin/bash

# pre-flight script for preparing the execution dir -- plain POSIX
# version. condor_chirp is used to obtain the input files, based
# on a list that the job supplied

set -e -u

printf 'preflight\n' > ./status
# minimum input/output setup
mkdir stamps
mkdir dataset

# if there is no input spec we can go home early
if [ ! -f ./input_files ]; then
  printf 'preflight_completed\n' > ./status
  touch stamps/prep_complete
  exit 0
fi

CONDOR_DIR=`condor_config_val LIBEXEC`
CONDOR_CHIRP="${CONDOR_DIR}/condor_chirp"

# with this preflight script we can only handle path locations, not URLs
DATASET_PATH=`cat ./source_dataset_location`

# obtain input files
while IFS= read -rd '' file; do
  mkdir -p dataset/"$(dirname ${file#${DATASET_PATH}})"
  "${CONDOR_CHIRP}" fetch "${file}" dataset/"${file#${DATASET_PATH}}"
done < ./input_files

printf 'preflight_completed\n' > ./status
touch stamps/prep_complete
