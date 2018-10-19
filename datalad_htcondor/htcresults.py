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
from datalad.support.constraints import EnsureNone
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


lgr = logging.getLogger('datalad.htcondor.htcresults')


@build_doc
class HTCResults(Interface):
    """TODO
    """
    _params_ = dict(
        # need to overwrite this one to get EnsureDataset return the right
        # instances
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
            doc=""""""),
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


