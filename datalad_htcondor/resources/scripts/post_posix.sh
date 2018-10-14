#!/bin/bash

# post-flight script to package job output for returning to the
# submission host -- plain POSIX version

set -e -u

touch POST_stamp

# TODO if outputfile specification is available in the exec dir
# loop over it and prepare a return package. If not, return everything
# that has changes
