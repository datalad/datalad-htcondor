#!/bin/bash

# post-flight script to package job output for returning to the
# submission host -- plain POSIX version

set -e -u

# TODO if outputfile specification is available in the exec dir
# loop over it and prepare a return package. If not, return everything
# that has changes
prep_stamp="stamps/prep_complete"

if [ -f "$prep_stamp" ]; then
    find "dataset" -type f,l -newer "$prep_stamp" > stamps/togethome
fi

tar --files-from stamps/togethome -czf output.tar.gz
