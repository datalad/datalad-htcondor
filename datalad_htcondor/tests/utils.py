import os
import time
from json import (
    loads,
    dumps,
)
from datalad.cmd import Runner
import datalad_revolution.utils as ut
from datalad.tests.utils import (
    assert_status,
)

job_status_map = {
    1: 'idle',
    2: 'running',
    3: 'removed',
    4: 'completed',
    5: 'held',
    6: 'submission_error',
}


def condor_q(proc_id):
    out, err = Runner().run(
        ['condor_q', '-json', proc_id],
        log_stdout=True,
        log_stderr=False,
        expect_fail=False,
    )
    if not out:
        return None
    rec = loads(out)
    return rec[0] if isinstance(rec, list) and len(rec) == 1 else rec


def submit_watcher(ds, **kwargs):
    res = ds.htc_prepare(
        submit=True,
        **kwargs
    )
    assert_status('ok', res)
    assert res[-1]['action'] == 'htc_submit'

    submission = res[-1]['submission']
    proc_ids = list(res[-1]['classads'].keys())
    # TODO it is a shame that we cannot pass pathobj through datalad yet
    submission_dir = ut.Path(res[-1]['path'])

    running = dict(zip(proc_ids, [True] * len(proc_ids)))
    error_msg = []
    while any(running.values()):
        for pid in proc_ids:
            q = condor_q(pid)
            if not q:
                running[pid] = False
                continue
            # job is still in the queue
            jstatus = job_status_map[q['JobStatus']]
            if jstatus in ('idle', 'running'):
                print('{}({})'.format(pid, jstatus))
            elif jstatus == 'held':
                print("ClassAd of held job {} follows:\n{}".format(
                    pid, dumps(q, indent=2)))
                error_msg.append('{} ({})'.format(q['HoldReason'], pid))
                os.system('condor_rm {}'.format(pid))
            elif jstatus == 'submission_error':
                error_msg.append('{} ({})'.format('submission error', pid))
        if error_msg:
            raise RuntimeError('Job does not run: %s', "; ".join(error_msg))
        time.sleep(3)

    return submission, submission_dir
