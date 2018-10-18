import time

from datalad.api import (
    rev_create as create,
    containers_add,
    htc_prepare,
)
from datalad_revolution.dataset import RevolutionDataset as Dataset
from datalad.tests.utils import (
    assert_result_count,
    with_tempfile,
)
from datalad.utils import on_windows


testimg_url = 'shub://datalad/datalad-container:testhelper'


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
        inputs=['*'],
        submit=True,
    )
    assert res[1]['action'] == 'htc_submit'
    submission_dir = res[0]['path']
    # we gotta wait till the results are in
    while not (submission_dir / 'job_0' / 'logs' / 'err').exists():
        time.sleep(1)
    time.sleep(2)
    assert (submission_dir / 'job_0' / 'output.tar.gz').exists()

