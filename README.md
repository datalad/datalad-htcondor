     ____          _           _                 _
    |  _ \   __ _ | |_   __ _ | |      __ _   __| |
    | | | | / _` || __| / _` || |     / _` | / _` |
    | |_| || (_| || |_ | (_| || |___ | (_| || (_| |
    |____/  \__,_| \__| \__,_||_____| \__,_| \__,_|
                                           HTCondor

[![Travis tests status](https://secure.travis-ci.org/datalad/datalad-htcondor.png?branch=master)](https://travis-ci.org/datalad/datalad-htcondor) [![codecov.io](https://codecov.io/github/datalad/datalad-htcondor/coverage.svg?branch=master)](https://codecov.io/github/datalad/datalad-htcondor?branch=master)

This extension enhances DataLad (http://datalad.org) with the ability for
remote execution via HTCondor. Please see the [extension
documentation](http://datalad-htcondor.rtfd.org)
for a description on additional commands and functionality.

For general information on how to use or contribute to DataLad (and this
extension), please see the [DataLad website](http://datalad.org) or the
[main GitHub project page](http://datalad.org).


## Installation

Before you install this package, please make sure that you [install a recent
version of git-annex](https://git-annex.branchable.com/install).  Afterwards,
install the latest version of `datalad-htcondor` from
[PyPi](https://pypi.org/project/datalad-htcondor). It is recommended to use
a dedicated [virtualenv](https://virtualenv.pypa.io):

    # create and enter a new virtual environment (optional)
    virtualenv --system-site-packages --python=python3 ~/env/dataladni
    . ~/env/dataladni/bin/activate

    # install from PyPi
    pip install datalad_htcondor


## Support

The documentation of this project is found here:
http://docs.datalad.org/projects/htcondor

All bugs, concerns and enhancement requests for this software can be submitted here:
https://github.com/datalad/datalad-htcondor/issues

If you have a problem or would like to ask a question about how to use DataLad,
please [submit a question to
NeuroStars.org](https://neurostars.org/tags/datalad) with a ``datalad`` tag.
NeuroStars.org is a platform similar to StackOverflow but dedicated to
neuroinformatics.

All previous DataLad questions are available here:
http://neurostars.org/tags/datalad/

## Acknowledgements

DataLad development is supported by a US-German collaboration in computational
neuroscience (CRCNS) project "DataGit: converging catalogues, warehouses, and
deployment logistics into a federated 'data distribution'" (Halchenko/Hanke),
co-funded by the US National Science Foundation (NSF 1429999) and the German
Federal Ministry of Education and Research (BMBF 01GQ1411). Additional support
is provided by the German federal state of Saxony-Anhalt and the European
Regional Development Fund (ERDF), Project: Center for Behavioral Brain
Sciences, Imaging Platform.  This work is further facilitated by the ReproNim
project (NIH 1P41EB019936-01A1).
