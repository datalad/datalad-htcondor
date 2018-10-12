#!/bin/bash

set -e -u

mkdir dataset
tar -C dataset -xf repo.tar
tar -C dataset -xf annex.tar

# make a standard reference
git -C dataset tag __datalad_submitted

$(condor_config_val LIBEXEC)/condor_chirp fetch /etc/passwd localfile
touch PRE_stamp
