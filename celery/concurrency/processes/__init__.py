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
from time import sleep

from billiard import forking_enable
from billiard.pool import RUN, CLOSE, Pool as _Pool
from billiard.queues import _SimpleQueue
from kombu.serialization import pickle as _pickle
from kombu.utils import fxrange
from kombu.utils.compat import get_errno

from celery import platforms
from celery import signals
from celery._state import set_default_app
from celery.concurrency.base import BasePool
from celery.task import trace
from celery.platforms import fileno
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

UNAVAIL = frozenset([errno.EAGAIN, errno.EINTR, errno.EBADF])

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


class promise(object):

    def __init__(self, fun, *partial_args, **partial_kwargs):
        self.fun = fun
        self.args = partial_args
        self.kwargs = partial_kwargs
        self.ready = False

    def __call__(self, *args, **kwargs):
        try:
            return self.fun(*tuple(self.args) + tuple(args),
                            **dict(self.kwargs, **kwargs))
        finally:
            self.ready = True


class AsynIPCPool(_Pool):

    def __init__(self, processes=None, *args, **kwargs):
        processes = self.cpu_count() if processes is None else processes
        self._queuepairs = dict((self.create_process_queuepair(), None)
                                for _ in range(processes))
        super(AsynIPCPool, self).__init__(processes, *args, **kwargs)

    def _available_queuepair(self):
        return next(pair for pair, owner in self._queuepairs.iteritems()
                    if owner is None)
    get_process_queuepair = _available_queuepair

    def create_process_queuepair(self):
        return _SimpleQueue(), _SimpleQueue()

    def _process_cleanup_queuepair(self, proc):
        try:
            self._queuepairs[self._find_worker_queuepair(proc)] = None
        except (KeyError, ValueError):
            pass

    def _process_register_queuepair(self, proc, pair):
        self._queuepairs[pair] = proc

    def _find_worker_queuepair(self, proc):
        for pair, owner in self._queuepairs.iteritems():
            if owner == proc:
                return pair
        raise ValueError(proc)

    def _setup_queues(self):
        self._inqueue = self._outqueue = \
            self._quick_put = self._quick_get = self._poll_result = None

    def on_partial_read(self, job, proc):
        resq = proc.outq._reader
        # empty result queue buffer
        while resq.poll(0):
            self.handle_result_event(resq.fileno())

        # job was not acked, so find another worker to send it to.
        if not job._accepted:
            self._put_back(job)

        # worker terminated by signal:
        # we cannot reuse the sockets again, because we don't know if the
        # process wrote/read anything from them, and if so we cannot
        # restore the message boundaries.
        if proc.exitcode < 0:
            print('JOB %r WRITTEN TO TERMINATED WORKER: %r' % (job, proc))
            for conn in (proc.inq, proc.outq):
                for sock in (conn._reader, conn._writer):
                    if not sock.closed:
                        os.close(sock.fileno())
            self._queuepairs[(proc.inq, proc.outq)] = \
                self._queuepairs[self.create_process_queuepair()] = None


class TaskPool(BasePool):
    """Multiprocessing Pool implementation."""
    Pool = AsynIPCPool

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
        self.terminate_job = self._pool.terminate_job
        self.grow = self._pool.grow
        self.shrink = self._pool.shrink
        self.restart = self._pool.restart
        self.maybe_handle_result = P._result_handler.handle_event
        self.outbound_buffer = deque()
        self.handle_result_event = P.handle_result_event
        self._all_inqueues = set(fileno(p.inq._writer) for p in P._pool)
        self._active_writes = set()
        self._active_writers = set()

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
        outbound = self.outbound_buffer
        pop_message = outbound.popleft
        put_message = outbound.append
        fileno_to_inq = self._pool._fileno_to_inq
        all_inqueues = self._all_inqueues
        active_writes = self._active_writes
        add_coro = hub.add_coro
        diff = all_inqueues.difference
        hub_add = hub.add
        mark_write_fd_as_active = active_writes.add
        mark_write_gen_as_active = self._active_writers.add
        write_generator_gone = self._active_writers.discard
        get_job = self._pool._cache.__getitem__
        self._pool._put_back = put_message

        for k, v in kwargs.iteritems():
            setattr(self._pool, k, v)

        def _write_to(fd, job, callback=None):
            header, body, body_size = job._payload
            try:
                try:
                    proc = fileno_to_inq[fd]
                except KeyError:
                    put_message(job)
                    raise StopIteration()
                send_offset = proc.inq._writer.send_offset
                # job result keeps track of what process the job is sent to.
                job._write_to = proc

                Hw = Bw = 0
                while Hw < 4:
                    try:
                        Hw += send_offset(header, Hw)
                    except Exception, exc:
                        if get_errno(exc) not in UNAVAIL:
                            raise
                        # suspend until more data
                        yield
                while Bw < body_size:
                    try:
                        Bw += send_offset(body, Bw)
                    except Exception, exc:
                        if get_errno(exc) not in UNAVAIL:
                            raise
                        # suspend until more data
                        yield
            finally:
                if callback:
                    callback()
                active_writes.discard(fd)

        def schedule_writes(ready_fd, events):
            try:
                job = pop_message()
            except IndexError:
                for inqfd in diff(active_writes):
                    hub.remove(inqfd)
            else:
                callback = promise(write_generator_gone)
                cor = _write_to(ready_fd, job, callback=callback)
                mark_write_gen_as_active(cor)
                mark_write_fd_as_active(ready_fd)
                callback.args = (cor, )  # tricky as we need to pass ref
                add_coro((ready_fd, ), cor, WRITE)

        def on_poll_start(hub):
            if outbound:
                hub_add(diff(active_writes), schedule_writes, hub.WRITE)
        self.on_poll_start = on_poll_start

        def quick_put(tup):
            body = dumps(tup, protocol=protocol)
            body_size = len(body)
            header = pack('>I', body_size)
            # index 0 is the job ID.
            job = get_job(tup[0])
            job._payload = header, buffer(body), body_size
            put_message(job)
        self._pool._quick_put = quick_put

    def handle_timeouts(self):
        if self._pool._timeout_handler:
            self._pool._timeout_handler.handle_event()

    def flush(self):
        if self.outbound_buffer:
            self.outbound_buffer.clear()
        try:
            # FLUSH OUTGOING BUFFERS
            intervals = fxrange(0.01, 0.1, 0.01, repeatlast=True)
            while self._active_writers:
                writers = list(self._active_writers)
                for gen in writers:
                    if gen.gi_frame.f_lasti != -1:  # generator started?
                        try:
                            next(gen)
                        except StopIteration:
                            self._active_writers.discard(gen)
                # workers may have exited in the meantime.
                # FIXME If a worker is terminated it could've been terminated
                # after having read partial data.
                self.maintain_pool()
                sleep(next(intervals))  # don't busyloop
        finally:
            self.outbound_buffer.clear()
            self._active_writers.clear()

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
