from datalad.api import (
    rev_create as create,
    containers_add,
    htc_prepare,
    htc_results,
)
from datalad_revolution.dataset import RevolutionDataset as Dataset
from datalad_revolution.tests.utils import (
    assert_repo_status,
)
from datalad.tests.utils import (
    with_tempfile,
    eq_,
    assert_status,
    assert_in,
)
from datalad_htcondor.tests.utils import submit_watcher


@with_tempfile
def test_basic(path):
    ds = Dataset(path).rev_create()

    submission, submission_dir = submit_watcher(
        ds,
        # TODO relative paths must work too!
        cmd='bash -c "ls -laR > here"',
        # '*' doesn't actually match anything, but we should be able
        # to simply not transfer anything in such a case
        inputs=['*'],
    )
    # no input_files spec was written
    assert not (submission_dir / 'input_files').exists()
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
    submission, submission_dir = submit_watcher(
        ds,
        cmd='bash -c "ls -laR > here2"'.format(ds.path),
        inputs=['*'],
    )
    assert (submission_dir / 'input_files').exists()
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
