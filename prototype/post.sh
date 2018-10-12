#!/bin/bash

set -e -u

# get all the patches out, probably only a single on in the
# standard case
git -C dataset format-patch -o .. __datalad_submitted

# which files have changed?
# TODO should be with --recursive but
# https://github.com/datalad/datalad/issues/2913
#datalad -f '{path}' diff --revision __datalad_submitted

# figure out what is actually in annex

touch POST_stamp
