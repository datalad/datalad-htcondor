#!/bin/bash

# post-flight script to package job output for returning to the
# submission host -- plain POSIX version

set -e -u

wdir="$(readlink -f .)"
printf "postflight" > "${wdir}/status"

# TODO if outputfile specification is available in the exec dir
# loop over it and prepare a return package. If not, return everything
# that has changes
prep_stamp="${wdir}/stamps/prep_complete"

# this next bit is not working
# it is intended to build an expression
# ( -path thisglob -o -path thatglob )
# for the find call below to match only desired outputs
# but I cannot get the quoting right to prevent the
# globs from being expanded to early, but still have
# find be able to act on them....arrrrgh
#selector=""
#if [ -f "${wdir}/output_globs" ]; then
#  while IFS= read -rd '' globexp; do
#    if [ -n "${selector}" ]; then
#      selector="${selector} -o"
#    fi
#    selector="${selector} -path '${globexp}'"
#  done < "${wdir}/output_globs"
#fi

# TODO check what reference point the output globs have and
# run `find` in that directory
# for now assume it is the dataset root
cd dataset
if [ -f "$prep_stamp" ]; then
  # intentionally use no starting point
  # TODO this is missing the selector expression
  # that is built (broken) above
  find \
    -type f,l \
    -newer "$prep_stamp" \
    > "${wdir}/stamps/togethome"
fi

[ -s "${wdir}/stamps/togethome" ] && tar \
  --files-from "${wdir}/stamps/togethome" \
  -czf "${wdir}/output" || touch "${wdir}/output"

printf "completed" > "${wdir}/status"
