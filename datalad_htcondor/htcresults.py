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
from six import (
    iteritems,
    text_type,
)

from pkg_resources import resource_string

from datalad.interface.base import (
    Interface,
    build_doc,
)
from datalad.interface.run import (
    GlobbedPaths,
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
    _params_ = dict(
        cmd=Parameter(
            args=("cmd",),
            doc="""""",
            constraints=EnsureChoice('list', 'merge')),
        dataset=Parameter(
            args=("-d", "--dataset"),
            doc="""specify the dataset to record the command results in.
            An attempt is made to identify the dataset based on the current
            working directory. If a dataset is given, the command will be
            executed in the root directory of this dataset.""",
            constraints=EnsureDataset() | EnsureNone()),
        submission=Parameter(
            args=("--submission",),
            doc=""""""),
        job=Parameter(
            args=("--job",),
            doc="""""",
            constraints=EnsureInt() | EnsureNone()),
    )

    @staticmethod
    @datasetmethod(name='htc_results')
    @eval_results
    def __call__(
            cmd,
            dataset=None,
            submission=None,
            job=None):

        ds = require_dataset(
            dataset,
            check_installed=True,
            purpose='handling results of remote command executions')

        if cmd == 'list':
            proc = list_results(ds, submission, job)
        elif cmd == 'merge':
            proc = merge_results(ds, submission, job)
        else:
            raise ValueError("unknown sub-command '{}'".format(cmd))

        for res in proc:
            yield res


def list_results(ds, submission, job):
    submissions_dir = get_submissions_dir(ds)
    if not submissions_dir.exists() or not submissions_dir.is_dir():
        return
    for p in submissions_dir.iterdir() \
            if submission is None \
            else [submissions_dir / 'submit_{}'.format(submission)]:
        if not p.is_dir() or not p.match('submit_*'):
            continue
        for j in p.iterdir() \
                if job is None else [p / 'job_{0:d}'.format(job)]:
            if not j.is_dir():
                continue
            submission_status_path = p / 'status'
            job_status_path = j / 'status'
            yield dict(
                action='htc_results',
                status='ok',
                state=job_status_path.read_text() if job_status_path.exists()
                else submission_status_path.read_text()
                if submission_status_path.exists() else None,
                path=text_type(j),
                submission=text_type(p.name)[7:],
                job=int(text_type(j.name)[4:]),
                refds=text_type(ds.pathobj),
                logger=lgr,
            )


def merge_results(ds, submission, job):
    yield
