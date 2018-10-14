#!/bin/sh
#
# wrapper script for running a singularity job as user 'nobody'
# to be used for execution jobs on a machine with no common
# user ids nor a shared file system wrt the submission host,

set -u -e

HOME="$(readlink -f .)"
export HOME

singularity exec --containall -H "$HOME" --pwd "$HOME" "$@"
