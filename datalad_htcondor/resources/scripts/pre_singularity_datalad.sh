#!/bin/bash

# pre-flight script for preparing the execution dir -- singularity
# container version. DataLad within a container is used to obtain
# the input files, based on a list that the job supplied

set -e -u

printf "preflight" > status
# minimum input/output setup
mkdir stamps

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
  singularity run \
    --containall -H "$(readlink -f .)" \
    ./datalad.simg install -s ${srcdataset} dataset
fi

# obtain input files
# actually use `datalad run` here with the original input args
# but a NOOP command, so nothing is actually changed.
# The outcome would be a dataset that is guaranteed to be as-ready as
# a local one would have been
singularity exec \
  --containall -H "$(readlink -f .)" \
  ./datalad.simg \
  python3 -c \
  'import os.path as op; from datalad.api import (run, Dataset); from datalad.support import json_py; ds=Dataset("dataset"); args=json_py.load("runargs.json"); ds.run(cmd=":", inputs=[op.relpath(i, args["pwd"]) for i in args.get("inputs", None)])'

printf "preflight_completed" > status
touch stamps/prep_complete