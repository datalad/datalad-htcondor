#!/bin/bash

# could be tmpdir
sdir='demo'

ds='dataset'
abs_ds="$(readlink -f $ds)"
ds_url="file://${abs_ds}"

# each arg forms its own job
job_args="$@"

./prep_jobs.sh "$sdir" ${#@}

git -C $sdir clone --depth 1 --no-single-branch --no-tags $ds_url repo

# obtain a shallow clone of any actually present subdataset
for subds in $(datalad -f '{path}' -C $abs_ds subdatasets -r --fulfilled yes); do
	relpath="$(realpath --relative-to=${abs_ds} $subds)"
	git -C $sdir/repo clone --depth 1 --no-single-branch --no-tags file://$subds $relpath
	# make git aware of the planted child
	# TODO when done in python, call this for the real parent
	git -C $sdir/repo submodule init $relpath
done
tar -C $sdir/repo -cvf $sdir/repo.tar .
rm -rf $sdir/repo

# suck in all annex objects into a dedicated tarball
find ${abs_ds} -type f -wholename '*git/annex/objects/*' -printf '%P\n' > filelist
tar -C ${abs_ds} -cvf $sdir/annex.tar --files-from=$(readlink -f filelist)
rm -f filelist


cat << EOT > $sdir/datalad.submit
# will become singularity job
Universe     = vanilla
Executable   = job.sh

+WantIOProxy = true

+SingularityImage = "sing.simg"

# all values for PRE and POST script MUST be quoted!
# name in execute dir on execute node
+PreCmd       = "pre.sh"
+PreArguments = "1"
#+PreEnvironment =
# name in execute dir on execute node
+PostCmd       = "post.sh"
+PostArguments = "1"
#+PostEnvironment =

# transfer and execution setup
run_as_owner = True
should_transfer_files = YES
when_to_transfer_output = ON_EXIT

# each job has its own dir, to receive the outputs back
initial_dir = job_\$(Process)

# paths must be relative to initial dir
transfer_input_files = ../pre.sh,../post.sh,../repo.tar,../annex.tar,../sing.simg

# paths are relative to a job's initial dir
Error   = logs/err
#Input   = logs/in
Output  = logs/out
Log     = logs/log

# and now for the actual specific job to run
# main job args to identify what is being done in this job
Arguments    = "1"
# launch
Queue

# another one
Arguments    = "3"
Queue
EOT

cp pre.sh $sdir
cp post.sh $sdir
cp job.sh $sdir

ln -s "/home/mih/hacking/datalad/htcondor/mih-ohbm2018-training-master-fsl.simg" $sdir/sing.simg
