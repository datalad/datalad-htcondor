import time

from datalad.api import (
    rev_create as create,
    containers_add,
    htc_prepare,
    htc_results,
)
from datalad_revolution.dataset import RevolutionDataset as Dataset
import datalad_revolution.utils as ut
from datalad_revolution.tests.utils import (
    assert_repo_status,
)
from datalad.tests.utils import (
    assert_result_count,
    with_tempfile,
    eq_,
    assert_status,
    assert_in,
)
from datalad.utils import on_windows
from datalad_htcondor.htcprepare import get_singularity_jobspec


# TODO implement job submission helper


@with_tempfile
def test_basic(path):
    ds = Dataset(path).rev_create()

    res = ds.htc_prepare(
        # TODO relative paths must work too!
        cmd='bash -c "ls -laR > here"',
        # '*' doesn't actually match anything, but we should be able
        # to simply not transfer anything in such a case
        inputs=['*'],
        submit=True,
    )
    assert res[-1]['action'] == 'htc_submit'
    # TODO it is a shame that we cannot pass pathobj through datalad yet
    submission_dir = ut.Path(res[-1]['path'])
    # no input_files spec was written
    assert not (submission_dir / 'input_files').exists()
    # we gotta wait till the results are in
    while not (submission_dir / 'job_0' / 'logs' / 'err').exists():
        time.sleep(1)
    time.sleep(2)
    assert (submission_dir / 'job_0' / 'output').exists()

    # add some content to the dataset and run again
    # this will generate a second (un-applied) job result in the
    # dataset store, but that should just work fine
    (ds.pathobj / 'myfile1.txt').write_text(u'dummy1')
    (ds.pathobj / 'myfile2.txt').write_text(u'dummy2')
    ds.rev_save()
    # all clean
    assert_repo_status(ds.path)
    # starting point
    start_commit = ds.repo.get_hexsha()
    res = ds.htc_prepare(
        cmd='bash -c "ls -laR > here2"'.format(ds.path),
        inputs=['*'],
        submit=True,
    )
    submission = res[-1]['submission']
    submission_dir = ut.Path(res[-1]['path'])
    assert (submission_dir / 'input_files').exists()
    # we gotta wait till the results are in
    while not (ds.htc_results(
            'list',
            submission=submission,
            job=0,
            return_type='item-or-list')['state'] == 'completed'):
        time.sleep(2)
    assert (submission_dir / 'job_0' / 'output').exists()

    # now apply the results to the original dataset
    assert_status('ok', ds.htc_results('merge', submission=submission))

    # we got exactly one commit out of this one
    eq_(start_commit, ds.repo.get_hexsha('HEAD~1'))

    # check whether the desired content is present
    outfile_path = ds.pathobj / 'here2'
    assert outfile_path.exists()
    ls_dump = outfile_path.read_text()
    # check that input files actually made it to the remote env
    assert_in('myfile1.txt', ls_dump)
    assert_in('myfile2.txt', ls_dump)

