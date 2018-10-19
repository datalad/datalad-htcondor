"""DataLad HTCondor extension"""

__docformat__ = 'restructuredtext'


# defines a datalad command suite
# this symbold must be indentified as a setuptools entrypoint
# to be found by datalad
command_suite = (
    # description of the command suite, displayed in cmdline help
    "Remote execution via HTCondor",
    [
        # specification of a command, any number of commands can be defined
        (
            # importable module that contains the command implementation
            'datalad_htcondor.htcprepare',
            # name of the command class implementation in above module
            'HTCPrepare',
            # optional name of the command in the cmdline API
            'htc-prepare',
            # optional name of the command in the Python API
            'htc_prepare'
        ),
    ]
)


from datalad import setup_package
from datalad import teardown_package
