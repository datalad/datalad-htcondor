# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Inspect and merge cluster job results"""

__docformat__ = 'restructuredtext'

import logging
import tempfile
import shutil
from six import (
    iteritems,
    text_type,
)

from pkg_resources import resource_string

import datalad.support.ansi_colors as ac
from datalad.interface.base import (
    Interface,
    build_doc,
)
from datalad.interface.run import (
    GlobbedPaths,
    _format_cmd_shorty,
)
from datalad.interface.utils import eval_results
from datalad.interface.results import get_status_dict
from datalad.support import json_py

from datalad.support.param import Parameter
from datalad.support.constraints import (
    EnsureNone,
    EnsureChoice,
    EnsureInt,
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
from datalad_htcondor.htcprepare import (
    get_submissions_dir,
)


lgr = logging.getLogger('datalad.htcondor.htcresults')


@build_doc
class HTCResults(Interface):
    """TODO
    """
    # make the custom renderer the default one, as the global default renderer
    # does not yield meaningful output for this command
    result_renderer = 'tailored'

    _params_ = dict(
        cmd=Parameter(
            args=("cmd",),
            metavar=("SUBCOMMAND",),
            nargs='?',
            doc="""""",
            constraints=EnsureChoice('list', 'merge', 'remove')),
        dataset=Parameter(
            args=("-d", "--dataset"),
            doc="""specify the dataset to record the command results in.
            An attempt is made to identify the dataset based on the current
            working directory. If a dataset is given, the command will be
            executed in the root directory of this dataset.""",
            constraints=EnsureDataset() | EnsureNone()),
        submission=Parameter(
            args=("submission",),
            nargs='?',
            metavar='SUBMISSION',
            doc=""""""),
        job=Parameter(
            args=("-j", "--job",),
            metavar='NUMBER',
            doc="""""",
            constraints=EnsureInt() | EnsureNone()),
        all=Parameter(
            args=("--all",),
            action='store_true',
            doc=""""""),
    )

    @staticmethod
    @datasetmethod(name='htc_results')
    @eval_results
    def __call__(
            cmd='list',
            dataset=None,
            submission=None,
            job=None,
            all=False):

        ds = require_dataset(
            dataset,
            check_installed=True,
            purpose='handling results of remote command executions')

        if cmd == 'list':
            jw = _list_job
            sw = _list_submission
#        elif cmd == 'merge':
#            proc = merge_results(ds, submission, job)
        elif cmd == 'remove':
            if not all and not submission and not job:
                raise ValueError(
                    "use the '--all' flag to remove all results across all "
                    "submissions")
            jw = _remove_dir
            sw = _remove_dir
        else:
            raise ValueError("unknown sub-command '{}'".format(cmd))

        for res in _doit(ds, submission, job, jw, sw):
            yield res

    @staticmethod
    def custom_result_renderer(res, **kwargs):  # pragma: no cover
        from datalad.ui import ui
        if not res['status'] == 'ok':
            # logging reported already
            return
        action = res['action'].split('_')[-1]
        ui.message('{action} {sub}{job}{state}{cmd}'.format(
            action=ac.color_word(action, kw_color_map.get(action, ac.WHITE))
            if action != 'list' else '',
            sub=res['submission'],
            job=' :{}'.format(res['job']) if 'job' in res else '',
            state=' [{}]'.format(
                ac.color_word(
                    res['state'],
                    kw_color_map.get(res['state'], ac.MAGENTA))
                if res.get('state', None) else 'unknown')
            if action =='list' else '',
            cmd=': {}'.format(
                _format_cmd_shorty(res['cmd']))
            if 'cmd' in res else '',
        ))


kw_color_map = {
    'remove': ac.RED,
    'merge': ac.GREEN,
    'completed': ac.GREEN,
    'submitted': ac.WHITE,
    'prepared': ac.YELLOW,
}


def _remove_dir(ds, dir, _ignored=None):
    common = dict(
        action='htc_result_remove',
        path=text_type(dir),
    )
    try:
        shutil.rmtree(dir)
        return dict(
            status='ok',
            **common)
    except Exception as e:
        return dict(
            status='error',
            message=("could not remove directory '%s': %s",
                     common['path'], exc_str(e)),
            **common)


def _list_job(ds, jdir, sdir):
    props = _list_submission(ds, sdir)
    job_status_path = jdir / 'status'
    return dict(
        props,
        state=job_status_path.read_text() if job_status_path.exists()
        else props.get('state', None),
        path=text_type(jdir),
    )


def _list_submission(ds, sdir):
    submission_status_path = sdir / 'status'
    args_path = sdir / 'runargs.json'
    if args_path.exists():
        try:
            cmd = json_py.load(args_path)['cmd']
        except Exception:
            cmd = None
    else:
        cmd = None
    return dict(
        action='htc_result_list',
        status='ok',
        state=submission_status_path.read_text()
        if submission_status_path.exists() else None,
        path=text_type(sdir),
        **(dict(cmd=cmd) if cmd else {})
    )


def _doit(ds, submission, job, jworker, sworker):
    common = dict(
        refds=text_type(ds.pathobj),
        logger=lgr,
    )
    submissions_dir = get_submissions_dir(ds)
    if not submissions_dir.exists() or not submissions_dir.is_dir():
        return
    if submission:
        sdir = submissions_dir / 'submit_{}'.format(submission)
        if not sdir.is_dir():
            yield dict(
                action='htc_results',
                status='error',
                path=text_type(sdir),
                message=("submission '%s' does not exist", submission),
                **common)
            return
    for p in submissions_dir.iterdir() \
            if submission is None \
            else [sdir]:
        if sworker is not None and job is None:
            yield dict(
                sworker(ds, p),
                submission=text_type(p.name)[7:],
                **common)
        if not p.is_dir() or not p.match('submit_*'):
            continue
        for j in p.iterdir() \
                if job is None else [p / 'job_{0:d}'.format(job)]:
            if not j.is_dir():
                continue
            yield dict(
                jworker(ds, j, p),
                submission=text_type(p.name)[7:],
                job=int(text_type(j.name)[4:]),
                **common)
