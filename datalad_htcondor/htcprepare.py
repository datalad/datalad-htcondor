# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Prepare a job(-cluster) submission for remote execution"""

__docformat__ = 'restructuredtext'


import logging
import tempfile
from six import (
    iteritems,
    text_type,
)
import shlex
import stat
import os.path as op

from pkg_resources import resource_string

from datalad.interface.base import (
    Interface,
    build_doc,
)
from datalad.interface.run import (
    Run,
    format_command,
    GlobbedPaths,
    prepare_inputs,
)
from datalad.interface.utils import eval_results
from datalad.interface.results import get_status_dict
from datalad.support import json_py

from datalad.support.param import Parameter
from datalad.support.constraints import (
    EnsureNone,
    EnsureChoice,
)
from datalad.support.exceptions import CommandError

from datalad.utils import get_dataset_pwds as get_command_pwds

from datalad.cmd import Runner

from datalad.dochelpers import exc_str

from datalad_revolution.gitrepo import RevolutionGitRepo as GitRepo

import datalad_revolution.utils as ut
from datalad_revolution.dataset import (
    datasetmethod,
    require_dataset,
    EnsureDataset,
)


lgr = logging.getLogger('datalad.htcondor.htcprepare')

harness_mapper = {
    'posixchirp': {
        'preflight_script': 'pre_posix_chirp.sh',
        'postflight_script': 'post_posix.sh',
    },
    'singularitydatalad': {
        'preflight_script': 'pre_singularity_datalad.sh',
        'postflight_script': 'post_posix.sh',
    },
}

submission_template = u"""\
Universe     = vanilla
Executable   = {executable}
environment  = "{environment}"

# true or false
+WantIOProxy = {ioproxy_flag}

# all values for PRE and POST script MUST be quoted!
# name in execute dir on execute node
+PreCmd       = "{preflight_script}"
+PreArguments = "{preflight_script_args}"
+PreEnvironment = "{preflight_script_env}"
+PostCmd       = "{postflight_script}"
+PostArguments = "{postflight_script_args}"
+PostEnvironment = "{postflight_script_env}"

# transfer and execution setup
run_as_owner = {run_as_owner_flag}
should_transfer_files = {transfer_files_mode}
when_to_transfer_output = {transfer_output_mode}

# each job has its own dir, to receive the outputs back
initial_dir = job_$(Process)

# paths must be relative to initial dir
transfer_input_files = {transfer_files_list}
transfer_output_files = status,stamps,output

# paths are relative to a job's initial dir
Error   = logs/err
# TODO support this case
#Input   = logs/in
Output  = logs/out
Log     = logs/log
"""


# defaults for the HTCondor submit file
# values must obey Condor syntax, not Python's
submission_defaults = dict(
    environment='',
    ioproxy_flag='true',
    # name in remote execute dir
    preflight_script='pre.sh',
    preflight_script_args='',
    preflight_script_env='',
    # name in remote execute dir
    postflight_script='post.sh',
    postflight_script_args='',
    postflight_script_env='',
    # TODO must be true ATM, although this is a bummer
    # with false the condor job runs as 'nobody' (with /nonexistent
    # declared as the homedir in /etc/passwd), but
    # singularity does not honor the HOME env var setting, hence
    # things fail unless singularity exe -H .. is given, which
    # condor doesn't do ATM
    run_as_owner_flag='false',
    transfer_files_mode='YES',
    transfer_output_mode='ON_EXIT',
)


def make_executable(pathobj):
    pathobj.chmod(
        stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH |
        stat.S_IWUSR |
        stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def get_singularity_jobspec(cmd):
    """Extract the runscript of a singularity container used as an executable

    Parameters
    ----------
    cmd : list
      A command as an argument list.

    Returns
    -------
    None or str, None or list
      If no singularity is available, or the executable in the command is not
      a singularity image given by its path, None is return. Otherwise the
      runscript of the container is returned a string. The second value is
      None if the first is None, or a list of arguments to the runscript.
    """
    # get the path to the command's executable
    exec_path = cmd[0]

    runner = Runner()
    if not op.exists(exec_path):
        # probably a command from PATH
        return

    # this is a real file, not just a command on the path
    try:
        stdout, stderr = runner.run(
            ['singularity', '--version'],
            log_stdout=True,
            log_stderr=True,
            expect_stderr=True,
            expect_fail=True,
        )
        # TODO could be used to tailor handling to particular versions
    except CommandError as e:  # pragma: no cover
        # we do not have a singularity installation that we can handle
        # log debug, because there is no guarantee that the executable
        # actually was a singularity container
        lgr.debug('No suitable singularity version installed: %s',
                  exc_str(e))
        return
    # we have singularity
    try:
        stdout, stderr = runner.run(
            # stringification only needed for pythons older than 3.6
            ['singularity', 'exec', exec_path,
             'cat', '/singularity'],
            log_stdout=True,
            log_stderr=True,
            expect_stderr=True,
            expect_fail=True,
        )
        # TODO could be used to tailor handling to particular versions
    except CommandError as e:
        # we do not have a singularity installation that we can handle
        # log debug, because there is no guarantee that the executable
        # actually was a singularity container
        lgr.debug('%s is not a singularity image: %s',
                  exec_path, exc_str(e))
        return
    # all but the container itself are the arguments
    return exec_path, cmd[1:]


def get_submissions_dir(ds):
    """Return pathobj of directory where all the submission packs live"""
    return ds.pathobj / GitRepo.get_git_dir(ds.path) / 'datalad' / 'htc'


@build_doc
class HTCPrepare(Interface):
    """TODO
    """
    _params_ = dict(
        {k: v for k, v in iteritems(Run._params_)
         if not k == 'rerun'},
        # need to overwrite this one to get EnsureDataset return the right
        # instances
        dataset=Parameter(
            args=("-d", "--dataset"),
            doc="""specify the dataset to record the command results in.
            An attempt is made to identify the dataset based on the current
            working directory. If a dataset is given, the command will be
            executed in the root directory of this dataset.""",
            constraints=EnsureDataset() | EnsureNone()),
        harness=Parameter(
            args=("--harness",),
            metavar='posixchirp|singularitydatalad',
            constraints=EnsureChoice('posixchirp', 'singularitydatalad'),
            doc="""harness for job input data preparation and result retrieval.
            The harness is responsible for preparing the remote execution
            environment to match the requirements of to-be-executed command,
            and to extract the obtained results after job execution has
            finished. 'posixchirp' (default): All input data is obtained via
            DataLad in the LOCAL environment, data transfer to the execution
            environment is done via HTCondor directly. The remote execution
            environment need not have DataLad available. 'singularitydatalad':
            DataLad is used in the REMOTE environment (from a Singularity
            container that is automatically obtained, if needed) to prepare
            execution. Only a minimal amount of data is transferred from the
            local machine directly."""),
        jobcfg=Parameter(
            args=("--jobcfg",),
            doc="""name of pre-crafted job configuration that is used to
            the tailor the HTCondor setup."""),
        submit=Parameter(
            args=("--submit",),
            action='store_true',
            doc="""if given, immediately submit the prepared submission"""),
    )

    @staticmethod
    @datasetmethod(name='htc_prepare')
    @eval_results
    def __call__(
            cmd=None,
            dataset=None,
            inputs=None,
            outputs=None,
            expand=None,
            explicit=False,
            message=None,
            sidecar=None,
            harness='posixchirp',
            jobcfg='default',
            submit=False):

        # TODO makes sure a different rel_pwd is handled properly on the remote end
        pwd, rel_pwd = get_command_pwds(dataset)

        ds = require_dataset(
            dataset,
            check_installed=True,
            purpose='preparing a remote command execution')

        try:
            cmd_expanded = format_command(ds,
                                          cmd,
                                          pwd=pwd,
                                          dspath=ds.path,
                                          inputs=inputs,
                                          outputs=outputs)
        except KeyError as exc:
            yield get_status_dict(
                'htcprepare',
                ds=ds,
                status='impossible',
                message=('command has an unrecognized placeholder: %s',
                         exc))
            return

        transfer_files_list = [
            'pre.sh', 'post.sh'
        ]

        # where all the submission packs live
        subroot_dir = get_submissions_dir(ds)
        subroot_dir.mkdir(parents=True, exist_ok=True)

        # location of to-be-created submission
        submission_dir = ut.Path(tempfile.mkdtemp(
            prefix='submit_', dir=text_type(subroot_dir)))
        submission = submission_dir.name[7:]

        split_cmd = shlex.split(cmd_expanded)
        # is this a singularity job?
        singularity_job = get_singularity_jobspec(split_cmd)
        if not singularity_job:
            with (submission_dir / 'runner.sh').open('wb') as f:
                f.write(resource_string(
                    'datalad_htcondor',
                    'resources/scripts/runner_direct.sh'))
            job_args = split_cmd
        else:
            # link the container into the submission dir
            # TODO with singularitydatalad harness this could be
            # done on the execution side
            (submission_dir / 'singularity.simg').symlink_to(
                ut.Path(singularity_job[0]).resolve())
            transfer_files_list.append('singularity.simg')
            # arguments of the job
            job_args = singularity_job[1]
            job_args.insert(0, 'singularity.simg')

            # TODO conditional on run_as_user=false
            with (submission_dir / 'runner.sh').open('wb') as f:
                f.write(resource_string(
                    'datalad_htcondor',
                    'resources/scripts/runner_singularity_anon.sh'))
        make_executable(submission_dir / 'runner.sh')

        # htcondor wants the log dir to exist at submit time
        # TODO ATM we only support a single job per cluster submission
        (submission_dir / 'job_0' / 'logs').mkdir(parents=True)

        with (submission_dir / 'pre.sh').open('wb') as f:
            f.write(resource_string(
                'datalad_htcondor',
                'resources/scripts/{}'.format(
                    harness_mapper[harness]['preflight_script'])))
        make_executable(submission_dir / 'pre.sh')

        with (submission_dir / 'post.sh').open('wb') as f:
            f.write(resource_string(
                'datalad_htcondor',
                'resources/scripts/{}'.format(
                    harness_mapper[harness]['postflight_script'])))
        make_executable(submission_dir / 'post.sh')

        # API support selection (bound dataset methods and such)
        # internal import to avoid circularities
        from datalad.api import (
            rev_status as status,
        )

        inputs = GlobbedPaths(inputs, pwd=pwd)
        # TODO make conditional on --harness selection
        prepare_inputs(ds, inputs)

        # it could be that an input expression does not expand,
        # because it doesn't match anything. In such a case
        # we need to filter out such globs to not confuse
        # the status() call below that only takes real paths
        inputs = [p for p in inputs.expand(full=True)
                  if op.lexists(p)]
        # now figure out what matches the remaining paths in the
        # entire repo and dump a list of files to transfer
        if inputs:
            with (submission_dir / 'input_files').open('w') as f:
                for p in ds.rev_status(
                        path=inputs,
                        # TODO do we really want that True? I doubt it
                        # this might pull in the world
                        recursive=False,
                        # we would have otherwise no idea
                        untracked='no',
                        result_renderer='disabled'):
                    f.write(text_type(p['path']))
                    f.write(u'\0')
                transfer_files_list.append('input_files')

        if outputs:
            # write the output globs to a file for eval on the execute
            # side
            # XXX we may not want to eval them on the remote side
            # at all, however. This would make things different
            # than with local execute, where we also just write to
            # a dataset and do not have an additional filter
            (submission_dir / 'output_globs').write_text(
                # we need a final trailing delimiter as a terminator
                u'\0'.join(outputs) + u'\0')
            transfer_files_list.append('output_globs')

        # TODO make conditional on switch to decide if execute node
        # can get the dataset from its 'origin' location too
        # if so just store the URL and switch to implied
        # --harness singularitydataset
        (submission_dir / 'source_dataset_location').write_text(
            text_type(ds.pathobj) + op.sep)
        transfer_files_list.append('source_dataset_location')

        with (submission_dir / 'cluster.submit').open('w') as f:
            f.write(submission_template.format(
                executable='runner.sh',
                # TODO if singularity_job else 'job.sh',
                transfer_files_list=','.join(
                    op.join(op.pardir, f) for f in transfer_files_list),
                **submission_defaults
            ))

            f.write(u'\narguments = "{}"\nqueue\n'.format(
                # TODO deal with single quotes in the args
                ' '.join("'{}'".format(a) for a in job_args)
            ))

        # dump the run command args into a file for re-use
        # when the result is merged
        # include even args that are already evaluated and
        # acted upon, to be able to convince `run` to create
        # a full run record that maybe could be re-run
        # locally
        json_py.dump(
            dict(
                cmd=cmd,
                inputs=inputs,
                outputs=outputs,
                expand=expand,
                explicit=explicit,
                message=message,
                sidecar=sidecar,
                # report the PWD to, to given `run` a chance
                # to be correct after the fact
                pwd=pwd,
            ),
            text_type(submission_dir / 'runargs.json')
        )

        # we use this file to inspect what state this submission is in
        (submission_dir / 'status').write_text(u'prepared')

        yield get_status_dict(
            action='htc_prepare',
            status='ok',
            refds=text_type(ds.pathobj),
            submission=submission,
            path=text_type(submission_dir),
            logger=lgr)

        if submit:
            try:
                Runner(cwd=text_type(submission_dir)).run(
                    ['condor_submit', 'cluster.submit'],
                    log_stdout=False,
                    log_stderr=False,
                    expect_stderr=True,
                    expect_fail=True,
                )
                (submission_dir / 'status').write_text(u'submitted')
                yield get_status_dict(
                    action='htc_submit',
                    status='ok',
                    submission=submission,
                    refds=text_type(ds.pathobj),
                    path=text_type(submission_dir),
                    logger=lgr)
            except CommandError as e:
                yield get_status_dict(
                    action='htc_submit',
                    status='error',
                    submission=submission,
                    message=('condor_submit failed: %s', exc_str(e)),
                    refds=text_type(ds.pathobj),
                    path=text_type(submission_dir),
                    logger=lgr)
