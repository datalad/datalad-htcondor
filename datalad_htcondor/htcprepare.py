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
from six import iteritems

from datalad.interface.base import Interface
from datalad.interface.run import Run
from datalad.interface.utils import eval_results
from datalad.interface.base import build_doc

from datalad.support.param import Parameter

from datalad.distribution.dataset import datasetmethod


lgr = logging.getLogger('datalad.htcondor.htcprepare')


@build_doc
class HTCPrepare(Interface):
    """TODO
    """
    _params_ = dict(
        {k: v for k, v in iteritems(Run._params_)
         if not k == 'rerun'},
        singularity=Parameter(
            args=("--singularity",),
            doc="""ADDME"""),
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
            singularity=None):
        if rerun:
            raise

        pass
