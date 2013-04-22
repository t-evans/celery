# -*- coding: utf-8 -*-
"""
    celery.concurrency.processes
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    Pool implementation using :mod:`multiprocessing`.

    We use the billiard fork of multiprocessing which contains
    numerous improvements.

"""
from __future__ import absolute_import

import errno
import os
import struct

from collections import deque
from pickle import HIGHEST_PROTOCOL

from billiard import forking_enable
from billiard.pool import Pool, RUN, CLOSE
from kombu.serialization import pickle as _pickle
from kombu.utils.compat import get_errno

from celery import platforms
from celery import signals
from celery._state import set_default_app
from celery.concurrency.base import BasePool
from celery.task import trace
from celery.utils.log import get_logger

from celery.worker.hub import WRITE

#: List of signals to reset when a child process starts.
WORKER_SIGRESET = frozenset(['SIGTERM',
                             'SIGHUP',
                             'SIGTTIN',
                             'SIGTTOU',
                             'SIGUSR1'])

#: List of signals to ignore when a child process starts.
WORKER_SIGIGNORE = frozenset(['SIGINT'])

UNAVAIL = frozenset([errno.EAGAIN, errno.EINTR])

MAXTASKS_NO_BILLIARD = """\
maxtasksperchild enabled but billiard C extension not installed!
This may lead to a deadlock, please install the billiard C extension.
"""

logger = get_logger(__name__)


def process_initializer(app, hostname):
    """Initializes the process so it can be used to process tasks."""
    platforms.signals.reset(*WORKER_SIGRESET)
    platforms.signals.ignore(*WORKER_SIGIGNORE)
    platforms.set_mp_process_title('celeryd', hostname=hostname)
    # This is for Windows and other platforms not supporting
    # fork(). Note that init_worker makes sure it's only
    # run once per process.
    app.loader.init_worker()
    app.loader.init_worker_process()
    app.log.setup(int(os.environ.get('CELERY_LOG_LEVEL', 0)),
                  os.environ.get('CELERY_LOG_FILE') or None,
                  bool(os.environ.get('CELERY_LOG_REDIRECT', False)),
                  str(os.environ.get('CELERY_LOG_REDIRECT_LEVEL')))
    if os.environ.get('FORKED_BY_MULTIPROCESSING'):
        # pool did execv after fork
        trace.setup_worker_optimizations(app)
    else:
        app.set_current()
        set_default_app(app)
        app.finalize()
        trace._tasks = app._tasks  # enables fast_trace_task optimization.
    from celery.task.trace import build_tracer
    for name, task in app.tasks.iteritems():
        task.__trace__ = build_tracer(name, task, app.loader, hostname)
    signals.worker_process_init.send(sender=None)


class TaskPool(BasePool):
    """Multiprocessing Pool implementation."""
    Pool = Pool

    requires_mediator = True
    uses_semaphore = True

    def on_start(self):
        """Run the task pool.

        Will pre-fork all workers so they're ready to accept tasks.

        """
        if self.options.get('maxtasksperchild'):
            try:
                import _billiard  # noqa
                _billiard.Connection.send_offset
            except (ImportError, AttributeError):
                # billiard C extension not installed
                logger.warning(MAXTASKS_NO_BILLIARD)

        forking_enable(self.forking_enable)
        P = self._pool = self.Pool(processes=self.limit,
                                   initializer=process_initializer,
                                   **self.options)
        self.on_apply = P.apply_async
        self.on_soft_timeout = P._timeout_handler.on_soft_timeout
        self.on_hard_timeout = P._timeout_handler.on_hard_timeout
        self.maintain_pool = P.maintain_pool
        self.maybe_handle_result = P._result_handler.handle_event
        self.outbound_buffer = deque()

    def did_start_ok(self):
        return self._pool.did_start_ok()

    def on_stop(self):
        """Gracefully stop the pool."""
        if self._pool is not None and self._pool._state in (RUN, CLOSE):
            self._pool.close()
            self._pool.join()
            self._pool = None

    def on_terminate(self):
        """Force terminate the pool."""
        if self._pool is not None:
            self._pool.terminate()
            self._pool = None

    def on_close(self):
        if self._pool is not None and self._pool._state == RUN:
            self._pool.close()

    def terminate_job(self, pid, signal=None):
        return self._pool.terminate_job(pid, signal)

    def grow(self, n=1):
        return self._pool.grow(n)

    def shrink(self, n=1):
        return self._pool.shrink(n)

    def restart(self):
        self._pool.restart()

    def _get_info(self):
        return {'max-concurrency': self.limit,
                'processes': [p.pid for p in self._pool._pool],
                'max-tasks-per-child': self._pool._maxtasksperchild,
                'put-guarded-by-semaphore': self.putlocks,
                'timeouts': (self._pool.soft_timeout, self._pool.timeout)}

    def init_callbacks(self, hub, **kwargs):
        protocol = HIGHEST_PROTOCOL
        pack = struct.pack
        dumps = _pickle.dumps
        inqueue_w = self._inqueue_w
        writer = self._inqueue_writer(hub)
        put_message = self.outbound_buffer.append

        def quick_put(obj):
            body = dumps(obj, protocol=protocol)
            body_size = len(body)
            header = pack('>I', body_size)
            put_message((header, buffer(body), body_size))
            return hub.add(inqueue_w.fileno(), writer, hub.WRITE)
        self._pool._quick_put = quick_put

        for k, v in kwargs.iteritems():
            setattr(self._pool, k, v)

    def handle_timeouts(self):
        if self._pool._timeout_handler:
            self._pool._timeout_handler.handle_event()

    def _inqueue_writer(self, hub, get_errno=get_errno):
        outbound = self.outbound_buffer
        pop = outbound.popleft
        send_offset = self._inqueue_w.send_offset

        while 1:
            while outbound:
                header, body, body_size = pop()
                Hw = Bw = 0
                while Hw < 4:
                    try:
                        Hw += send_offset(header, Hw)
                    except Exception, exc:
                        if get_errno(exc) not in UNAVAIL:
                            raise
                        # suspend until more data
                        yield WRITE
                while Bw < body_size:
                    try:
                        Bw += send_offset(body, Bw)
                    except Exception, exc:
                        if get_errno(exc) not in UNAVAIL:
                            raise
                        # suspend until more data
                        yield WRITE
            yield  # removes itself from evloop

    @property
    def num_processes(self):
        return self._pool._processes

    @property
    def readers(self):
        return self._pool.readers

    @property
    def writers(self):
        return self._pool.writers

    @property
    def timers(self):
        return {self.maintain_pool: 5.0}

    @property
    def _inqueue_w(self):
        return self._pool._inqueue._writer
