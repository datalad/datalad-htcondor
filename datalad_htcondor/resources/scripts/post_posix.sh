#!/bin/bash

# post-flight script to package job output for returning to the
# submission host -- plain POSIX version

set -e -u

wdir="$(readlink -f .)"

# TODO if outputfile specification is available in the exec dir
# loop over it and prepare a return package. If not, return everything
# that has changes
prep_stamp="${wdir}/stamps/prep_complete"


# TODO check what reference point the output globs have and
# run `find` in that directory
# for now assume it is the dataset root
cd dataset
if [ -f "$prep_stamp" ]; then
  # intentionally use no starting point
  find \
    -type f,l \
    -newer "$prep_stamp" \
    > "${wdir}/stamps/togethome"
fi

tar \
  --files-from "${wdir}/stamps/togethome" \
  -czf "${wdir}/output.tar.gz"
