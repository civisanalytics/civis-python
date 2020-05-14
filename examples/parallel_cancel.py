#!/usr/bin/env python3

import time
from joblib import (
    Parallel,
    parallel_backend,
    delayed,
    register_parallel_backend
)
import civis


def test_job(n):
    time.sleep(120)
    print(n)


backend_type = 'civis_parallel_backend'
register_parallel_backend(
    backend_type,
    civis.parallel.make_backend_factory(
        name='Civis parallel test',
        polling_interval=10,
        hidden=False,
    )
)

n_jobs = 3
with parallel_backend(backend_type):
    parallel = Parallel(n_jobs=n_jobs, pre_dispatch='n_jobs',
                        verbose=100)

    fn = delayed(test_job)
    results = parallel(
        fn(i) for i in range(n_jobs)
    )

    print(results)
