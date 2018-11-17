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
from datalad.interface.common_cfg import dirs as dirs
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
transfer_output_files = status,stamps,output,preflight.log,postflight.log

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
    preflight_script='prepost_runner.sh',
    preflight_script_args="-c './pre.sh > preflight.log 2>&1'",
    preflight_script_env='',
    # name in remote execute dir
    postflight_script='prepost_runner.sh',
    postflight_script_args="-c './post.sh > postflight.log 2>&1'",
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
    """Prepare a command for remote execution

    *Notes on command behavior*

    Caching of DataLad singularity container
      With the 'singularitydatalad' harness a singularity container with
      a DataLad installation is needed. If no container image is sent
      with the job, an image will be automatically downloaded from singularity
      hub in the execution environment. If that is not desired (too slow,
      forbidden, custom image is desired, etc), A locally cached container
      image can be used instead. The configuarion variable
      'datalad.htcprepare.datalad-simg' is queried and if the configured
      path (default: $HOME/.cache/datalad/htcondor/datalad.simg) points to
      an existing file, it will be transferred to serve as the containter
      image to run preparation and inspection before and after the job
      ran.
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
            constraints=EnsureChoice('posixchirp', 'singularitydatalad') | EnsureNone(),
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
        from_sibling=Parameter(
            args=("-s", "--from-sibling",),
            metavar='NAME',
            doc="""remote execution will pull all information (incl. the
            dataset repository) from this configured dataset sibling"""),
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
            harness=None,
            from_sibling=None,
            jobcfg='default',
            submit=False):

        # API support selection (bound dataset methods and such)
        # internal import to avoid circularities
        from datalad.api import (
            rev_status as status,
            siblings,
        )

        # basic and cheap sanity checks first
        if not cmd:
            # possibly relax this later on, when a command could
            # come from a config setting
            raise ValueError('no command specified')

        # TODO makes sure a different rel_pwd is handled properly on the remote end
        pwd, rel_pwd = get_command_pwds(dataset)

        ds = require_dataset(
            dataset,
            check_installed=True,
            purpose='preparing a remote command execution')

        common_res = get_status_dict(
            'htcprepare', ds=ds, logger=lgr)

        # what will always be transferred
        transfer_files_list = [
            'pre.sh', 'post.sh', 'runargs.json', 'prepost_runner.sh'
        ]

        # where all the submission packs live
        subroot_dir = get_submissions_dir(ds)
        subroot_dir.mkdir(parents=True, exist_ok=True)

        # location of to-be-created submission
        submission_dir = ut.Path(tempfile.mkdtemp(
            prefix='submit_', dir=text_type(subroot_dir)))
        submission = submission_dir.name[7:]

        if from_sibling:
            if harness not in (None, 'singularitydatalad'):
                raise ValueError(
                    '--harness selection conflicts with --from-sibling, ',
                    'implies --harness singularitydatalad')
            harness = 'singularitydatalad'
            # first check repo state, no need to go recursive, subdataset
            # will show up as tainted anyways
            if ds.repo.diff(fr='HEAD', to=None):
                yield dict(
                    common_res,
                    status='error',
                    message=\
                    'the local dataset state can not be obtained from '
                    'a remote sibling, as the dataset has unsaved changes: '
                    'save changes and publish them first.')
                return
            sibling = ds.siblings(
                name=from_sibling,
                get_annex_info=False,
                result_renderer='disabled',
                return_type='item-or-list',
                on_failure='ignore')
            if not sibling or sibling.get('status', None) != 'ok' \
                    or 'url' not in sibling or not sibling['url']:
                yield dict(
                    common_res,
                    status='error',
                    message=("sibling '%s' not available as a remote source",
                             from_sibling))
                return
            # TODO, bring back test with a git fetch remote commit test
            # to not have a job fail with a cryptic message

            # work with the URL from now
            from_sibling = sibling['url']
        elif harness == 'singularitydatalad':
            # this harness needs a repository on the execution side
            # as --from-sibling wasn't given, we need to create a
            # lightweight one to be sent along with the job
            transfer_files_list.append('dataset')
            # make a shallow clone, that includes the tip of all branch
            # (important to have at least the git-annex branch in
            # addition to what is checked out)
            Runner().run(
                ['git', 'clone', '--depth', '1', '--no-single-branch',
                 ds.path, str(submission_dir / 'dataset')],
                expect_fail=False,
            )
        if harness is None:
            harness = 'posixchirp'
        elif harness == 'singularitydatalad':
            # rec record the local HEAD commit in the submission
            # the preflight script will verify that it can be checked out
            # or will blow up
            (submission_dir / 'commit').write_text(
                ds.repo.get_hexsha())
            transfer_files_list.append('commit')

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

        # we always need the runner for the pre/post scripts
        with (submission_dir / 'prepost_runner.sh').open('wb') as f:
            f.write(resource_string(
                'datalad_htcondor',
                'resources/scripts/prepost_runner.sh'))
        make_executable(submission_dir / 'prepost_runner.sh')
        if harness == 'singularitydatalad':
            # do we have an image cached at the configured
            # location
            dlsimg_location = ds.config.obtain(
                'datalad.htcprepare.datalad-simg',
                default=str(ut.Path(dirs.user_cache_dir) /
                    'htcondor' / 'datalad.simg'))
            if op.exists(dlsimg_location):
                lgr.info("Using cached DataLad singularity from '%s'",
                         dlsimg_location)
                (submission_dir / 'datalad.simg').symlink_to(
                    ut.Path(dlsimg_location).resolve())
                transfer_files_list.append('datalad.simg')
            else:
                lgr.info("No cached DataLad singularity found at '%s', "
                         "will be downloaded from Singularity-Hub "
                         "in the remote environment.",
                         dlsimg_location)

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
        # but not if we get everything from a remote anyways
        if not from_sibling and inputs:
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

        # TODO switch to implied --harness singularitydataset with
        # from_sibling
        (submission_dir / 'source_dataset_location').write_text(
            from_sibling
            if from_sibling
            else (text_type(ds.pathobj) + op.sep))
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
                out, err = Runner(cwd=text_type(submission_dir)).run(
                    ['condor_submit', '-verbose', 'cluster.submit'],
                    log_stdout=True,
                    log_stderr=False,
                    expect_stderr=True,
                    expect_fail=True,
                )
            except CommandError as e:
                yield get_status_dict(
                    action='htc_submit',
                    status='error',
                    submission=submission,
                    message=('condor_submit failed: %s', exc_str(e)),
                    refds=text_type(ds.pathobj),
                    path=text_type(submission_dir),
                    logger=lgr)
                return
            # some ad-hoc classad parser to report maximum info
            classads = None
            classad = None
            for line in out.splitlines():
                if line.startswith('** '):
                    if classads is None:
                        classads = {}
                    classad = {}
                    classads[line[8:-1]] = classad
                if classads is None or not line.strip():
                    continue
                try:
                    sepidx = line.index('=')
                except ValueError:
                    # cannot handle this
                    continue
                classad[line[:sepidx - 1]] = line[sepidx + 2:].strip('"')

            (submission_dir / 'status').write_text(u'submitted')
            yield get_status_dict(
                action='htc_submit',
                status='ok',
                submission=submission,
                classads=classads,
                refds=text_type(ds.pathobj),
                path=text_type(submission_dir),
                logger=lgr)
