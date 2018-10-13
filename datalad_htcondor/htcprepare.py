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


submission_template = """\
Universe     = vanilla
Executable   = {executable}

# true or false
+WantIOProxy = {ioproxy_flag}

#+SingularityImage = "sing.simg"
{singularityimg_spec}

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
initial_dir = job_\$(Process)

# paths must be relative to initial dir
transfer_input_files = {transfer_files_list}

# paths are relative to a job's initial dir
Error   = logs/err
# TODO support this case
#Input   = logs/in
Output  = logs/out
Log     = logs/log
"""


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
        jobcfg=Parameter(
            args=("--jobcfg",),
            doc="""name of pre-crafted job configuration that is used to
            the tailor the HTCondor setup."""),
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
            singularity=None,
            jobcfg=None):
        pass
