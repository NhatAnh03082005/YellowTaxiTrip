"""Structured timings; never log rows, connection strings or credentials."""
import json
try:
    import resource
except ImportError:  # Windows authoring/test environment
    resource = None
import time
from contextlib import contextmanager
from functools import wraps


@contextmanager
def phase(name, **labels):
    started = time.perf_counter()
    status = 'SUCCESS'
    try:
        yield
    except BaseException:
        status = 'FAILED'
        raise
    finally:
        print('MAGE_PROFILE ' + json.dumps({
            'phase': name, 'status': status, 'started_unix': time.time() - (time.perf_counter()-started),
            'seconds': round(time.perf_counter()-started, 6),
            'process_peak_rss_kib': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss if resource else None,
            **labels,
        }, sort_keys=True), flush=True)


def profiled(function):
    @wraps(function)
    def wrapped(*args, **kwargs):
        with phase(function.__name__):
            return function(*args, **kwargs)
    return wrapped
