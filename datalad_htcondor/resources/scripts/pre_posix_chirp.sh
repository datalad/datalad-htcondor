#!/bin/bash

# pre-flight script for preparing the execution dir -- plain POSIX
# version. condor_chirp is used to obtain the input files, based
# on a list that the job supplied

set -e -u

ls -la >> PRE_stamp

# TODO if inputfile specification is available in the exec dir
# loop over it and obtain all files via condor_chirp
#$(condor_config_val LIBEXEC)/condor_chirp fetch /etc/passwd localfile
