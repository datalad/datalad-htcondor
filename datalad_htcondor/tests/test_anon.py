import time

from datalad.api import (
    rev_create as create,
    containers_add,
    htc_prepare,
)
from datalad_revolution.dataset import RevolutionDataset as Dataset
import datalad_revolution.utils as ut
from datalad.tests.utils import (
    assert_result_count,
    with_tempfile,
)
from datalad.utils import on_windows


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
    res = ds.htc_prepare(
        # TODO relative paths must work too!
        cmd='{}/.datalad/environments/mycontainer/image bash -c "ls -laR > here"'.format(ds.path),
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
    assert (submission_dir / 'job_0' / 'output.tar.gz').exists()

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
    assert (submission_dir / 'job_0' / 'output.tar.gz').exists()
