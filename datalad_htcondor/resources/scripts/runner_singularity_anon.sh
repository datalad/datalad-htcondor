#!/bin/sh
#
# wrapper script for running a singularity job as user 'nobody'
# to be used for execution jobs on a machine with no common
# user ids nor a shared file system wrt the submission host,

set -u -e

HOME="$(readlink -f .)"
export HOME

# have an artificial home for the nobody user and make payload
# run in the root of the dataset inside the container
singularity exec \
  --containall -H "$HOME" \
  -B "$(readlink -f dataset)":"/dataset" \
  --pwd "/dataset" \
  "$@"
