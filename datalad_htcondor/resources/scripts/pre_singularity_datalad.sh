#!/bin/bash

# pre-flight script for preparing the execution dir -- singularity
# container version. DataLad within a container is used to obtain
# the input files, based on a list that the job supplied

set -e -u

printf "preflight" > status
# minimum input/output setup
mkdir stamps

# if there is no input spec we can go home early
if [ ! -f input_files ]; then
  printf "preflight_completed" > status
  touch stamps/prep_complete
  # have a starting point for the job and the postflight script
  mkdir dataset
  exit 0
fi

# if the datalad singularity image did not come with the job, get it
# from singularity-hub
if [ ! -f datalad.simg ]; then
  singularity pull -n datalad.simg shub://datalad/datalad-htcondor:latest
  chmod +x datalad.simg
fi

# where to get the dataset from, can be anything that datalad install
# can handle
srcdataset="$(cat source_dataset_location)"

# if srcdataset is set to 'dataset' the dataset has already been sent
# with the submission, and we don't need to obtain it
if [ x"${srcdataset}" != xdataset ]; then
  ./datalad.simg install -s ${srcdataset} dataset
fi

# obtain input files
# going through xargs will automatically adjust the number of calls
# to `get` to not exceed the platform limit on max command length
xargs --null --arg-file input_files ./datalad.simg get -d dataset

printf "preflight_completed" > status
touch stamps/prep_complete
