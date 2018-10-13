#!/usr/bin/env python

import os.path as op
from setuptools import setup
from setuptools import find_packages


def get_version():
    # This might entail lots of imports which might not yet be available
    # so let's do ad-hoc parsing of the version.py
    with open(op.join(
            op.dirname(__file__),
            'datalad_htcondor',
            'version.py')) as f:
        version_lines = list(filter(lambda x: x.startswith('__version__'), f))
    assert (len(version_lines) == 1)
    return version_lines[0].split('=')[1].strip(" '\"\t\n")

setup(
    # basic project properties can be set arbitrarily
    name="datalad_htcondor",
    author="The DataLad Team and Contributors",
    author_email="team@datalad.org",
    version=get_version(),
    description="DataLad extension for remote execution via HTCondor",
    packages=[pkg for pkg in find_packages('.') if pkg.startswith('datalad')],
    # datalad command suite specs from here
    install_requires=[
        'datalad',
    ],
    entry_points = {
        # 'datalad.extensions' is THE entrypoint inspected by the datalad API builders
        'datalad.extensions': [
            # the label in front of '=' is the command suite label
            # the entrypoint can point to any symbol of any name, as long it is
            # valid datalad interface specification (see demo in this extensions
            'htcondor=datalad_htcondor:command_suite',
        ]
    },
)
