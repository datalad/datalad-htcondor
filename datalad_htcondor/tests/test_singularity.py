import time

from datalad.api import (
    rev_create as create,
    containers_add,
    htc_prepare,
    install,
)
from datalad_revolution.dataset import RevolutionDataset as Dataset
import datalad_revolution.utils as ut
from datalad.tests.utils import (
    assert_result_count,
    with_tempfile,
    eq_,
    assert_status,
    assert_in,
    assert_not_in,
)
from datalad.utils import on_windows
from datalad_htcondor.htcprepare import get_singularity_jobspec


testimg_url = 'shub://datalad/datalad-container:testhelper'

# TODO implement job submission helper


@with_tempfile
def test_basic(path):
    ds = Dataset(path).rev_create()
    # plug in a proper singularity image
    ds.containers_add(
        'mycontainer',
        url=testimg_url,
    )
    imgpath = '{}/.datalad/environments/mycontainer/image'.format(ds.path)

    # must come back with None in all kinds of scenarios that do not
    # involve a sigularity image
    # something that is not a path to an existing thing
    eq_(None, get_singularity_jobspec(['bash']))
    # something that singularity rejects as an image
    eq_(None, get_singularity_jobspec(['{}/.datalad/config'.format(ds.path)]))
    # finally a real image
    img, rest = get_singularity_jobspec(['{}'.format(imgpath), 'rest1', 'rest2'])
    eq_(str(img), imgpath)
    eq_(rest, ['rest1', 'rest2'])

    res = ds.htc_prepare(
        # TODO relative paths must work too!
        cmd='{} bash -c "ls -laR > here"'.format(imgpath),
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
    res = ds.htc_prepare(
        # TODO relative paths must work too!
        cmd='{}/.datalad/environments/mycontainer/image bash -c "ls -laR > here"'.format(ds.path),
        inputs=['*'],
        submit=True,
    )
    assert res[-1]['action'] == 'htc_submit'
    # TODO it is a shame that we cannot pass pathobj through datalad yet
    submission_dir = ut.Path(res[-1]['path'])
    # no input_files spec was written
    assert (submission_dir / 'input_files').exists()
    # we gotta wait till the results are in
    while not (submission_dir / 'job_0' / 'logs' / 'err').exists():
        time.sleep(1)
    time.sleep(2)
    assert (submission_dir / 'job_0' / 'output').exists()




@with_tempfile
def test_singularitydatalad_harness(path):
    ds = Dataset(path).rev_create()

    (ds.pathobj / 'myfile1.txt').write_text(u'dummy1')
    (ds.pathobj / 'myfile2.txt').write_text(u'dummy2')
    ds.rev_save()
    res = ds.htc_prepare(
        cmd='bash -c "ls -laR > here"'.format(ds.path),
        inputs=['*'],
        harness='singularitydatalad',
        submit=True,
    )
    assert res[-1]['action'] == 'htc_submit'
    # TODO it is a shame that we cannot pass pathobj through datalad yet
    submission_dir = ut.Path(res[-1]['path'])
    # no input_files spec was written
    assert (submission_dir / 'input_files').exists()
    # we gotta wait till the results are in
    while not (submission_dir / 'job_0' / 'logs' / 'err').exists():
        time.sleep(1)
    time.sleep(2)
    assert (submission_dir / 'job_0' / 'output').exists()


@with_tempfile
def test_from_remote_sibling(path):
    install(source='https://github.com/datalad/datalad-testrepo.git',
            path=path)
    # install does not give us next-gen datasets
    ds = Dataset(path)
    start_commit = ds.repo.get_hexsha()
    res = ds.htc_prepare(
        cmd='bash -c "file -L {inputs} > result"',
        inputs=['inannex/animated.gif'],
        harness='singularitydatalad',
        from_sibling='origin',
        submit=True,
    )
    assert_status('ok', res)

    # we gotta wait till the results are in
    submission = res[-1]['submission']
    submission_dir = ut.Path(res[-1]['path'])
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
    outfile_path = ds.pathobj / 'result'
    assert outfile_path.exists()
    assert_in('GIF image', outfile_path.read_text())

    # check that no longer get was performed
    # because it lives in a 'web' special remote, the local annex
    # doesn't even have a key for this file
    # (it would have, after it was obtain)
    assert_not_in(
        'key',
        ds.repo.annexstatus(paths=['inannex/animated.gif']))
