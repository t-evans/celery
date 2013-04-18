# -*- coding: utf-8 -*-
"""
    celery.worker.hub
    ~~~~~~~~~~~~~~~~~

    Event-loop implementation.

"""
from __future__ import absolute_import

from kombu.utils import cached_property
from kombu.utils import eventio

from collections import defaultdict, deque

from celery.platforms import fileno
from celery.utils.log import get_logger
from celery.utils.timer2 import Schedule

logger = get_logger(__name__)
READ, WRITE, ERR = eventio.READ, eventio.WRITE, eventio.ERR


def repr_flag(flag):
    return '%s%s%s' % ('R' if flag & READ else '',
                       'W' if flag & WRITE else '',
                       '!' if flag & ERR else '')


def _rcb(obj):
    if isinstance(obj, trampoline):
        return repr(obj)
    return obj.__name__


class BoundedSemaphore(object):
    """Asynchronous Bounded Semaphore.

    Bounded means that the value will stay within the specified
    range even if it is released more times than it was acquired.

    This type is *not thread safe*.

    Example:

        >>> x = BoundedSemaphore(2)

        >>> def callback(i):
        ...     say('HELLO %r' % (i, ))

        >>> x.acquire(callback, 1)
        HELLO 1

        >>> x.acquire(callback, 2)
        HELLO 2

        >>> x.acquire(callback, 3)
        >>> x._waiters   # private, do not access directly
        [(callback, 3)]

        >>> x.release()
        HELLO 3

    """

    def __init__(self, value):
        self.initial_value = self.value = value
        self._waiting = []

    def acquire(self, callback, *partial_args):
        """Acquire semaphore, applying ``callback`` when
        the semaphore is ready.

        :param callback: The callback to apply.
        :param \*partial_args: partial arguments to callback.

        """
        if self.value <= 0:
            self._waiting.append((callback, partial_args))
            return False
        else:
            self.value = max(self.value - 1, 0)
            callback(*partial_args)
            return True

    def release(self):
        """Release semaphore.

        This will apply any waiting callbacks from previous
        calls to :meth:`acquire` done when the semaphore was busy.

        """
        self.value = min(self.value + 1, self.initial_value)
        if self._waiting:
            waiter, args = self._waiting.pop()
            waiter(*args)

    def grow(self, n=1):
        """Change the size of the semaphore to hold more values."""
        self.initial_value += n
        self.value += n
        [self.release() for _ in xrange(n)]

    def shrink(self, n=1):
        """Change the size of the semaphore to hold less values."""
        self.initial_value = max(self.initial_value - n, 0)
        self.value = max(self.value - n, 0)

    def clear(self):
        """Reset the sempahore, including wiping out any waiting callbacks."""
        self._waiting[:] = []
        self.value = self.initial_value


class trampoline(object):

    def __init__(self, hub, fd, it, write=False):
        self.hub = hub
        self.fd = fd
        self.it = it
        self.write = write
        self.ready = self.failed = False
        self.__name__ = it.__name__

        hub.add(fd, self, WRITE if write else (READ | ERR))

    def __call__(self, fd=None, event=None):
        try:
            val = next(self.it)
        except StopIteration:
            self.hub.remove(self.fd)
            self.ready = True
        except BaseException:
            self.hub.remove(self.fd)
            self.ready = self.failed = True
            raise
        else:
            if val:
                return self.hub.remove(self.fd)
            self.hub.add(self.fd, self, WRITE if self.write else (READ | ERR))

    def __repr__(self):
        return '&%s(%s)' % (self.__name__,
                            'ready' if self.ready else 'pending')


class Hub(object):
    """Event loop object.

    :keyword timer: Specify custom :class:`~celery.utils.timer2.Schedule`.

    """
    #: Flag set if reading from an fd will not block.
    READ = READ

    #: Flag set if writing to an fd will not block.
    WRITE = WRITE

    #: Flag set on error, and the fd should be read from asap.
    ERR = ERR

    #: List of callbacks to be called when the loop is initialized,
    #: applied with the hub instance as sole argument.
    on_init = None

    #: List of callbacks to be called when the loop is exiting,
    #: applied with the hub instance as sole argument.
    on_close = None

    #: List of callbacks to be called when a task is received.
    #: Takes no arguments.
    on_task = None

    def __init__(self, timer=None):
        self.timer = Schedule() if timer is None else timer

        self.readers = {}
        self.writers = {}
        self.on_init = []
        self.on_close = []
        self.on_task = []

    def start(self):
        """Called by StartStopComponent at worker startup."""
        self.poller = eventio.poll()

    def stop(self):
        """Called by StartStopComponent at worker shutdown."""
        self.poller.close()

    def init(self):
        for callback in self.on_init:
            callback(self)

    def _callback_for(self, fd, flag):
        if flag & READ:
            return self.readers[fileno(fd)]
        elif flag & WRITE:
            return self.writers[fileno(fd)]

    def fire_timers(self, min_delay=1, max_delay=10, max_timers=10,
                    propagate=()):
        delay = None
        if self.timer._queue:
            for i in range(max_timers):
                delay, entry = self.scheduler.next()
                if entry is None:
                    break
                try:
                    entry()
                except propagate:
                    raise
                except Exception, exc:
                    logger.error('Error in timer: %r', exc, exc_info=1)
        return min(max(delay or 0, min_delay), max_delay)

    def add(self, fd, callback, flags):
        try:
            self.poller.register(fd, flags)
        except ValueError:
            self._discard(fd)
        else:
            d = self.readers if flags & READ else self.writers
            d[fileno(fd)] = callback

    def remove(self, fd):
        self._unregister(fd)
        self._discard(fd)

    def trampoline(self, fd, it, write=False):
        return trampoline(self, fd, it, write=write)

    def when_readable(self, fd, it):
        return self.trampoline(fd, it, write=False)

    def when_writable(self, fd, it):
        return self.trampoline(fd, it, write=True)

    def add_reader(self, fd, callback):
        return self.add(fd, callback, READ | ERR)

    def add_writer(self, fd, callback):
        return self.add(fd, callback, WRITE)

    def update_readers(self, readers):
        [self.add_reader(*x) for x in readers.iteritems()]

    def update_writers(self, writers):
        [self.add_writer(*x) for x in writers.iteritems()]

    def _unregister(self, fd):
        try:
            self.poller.unregister(fd)
        except (KeyError, OSError):
            pass

    def _discard(self, fd):
        fd = fileno(fd)
        self.readers.pop(fd, None)
        self.writers.pop(fd, None)

    def __enter__(self):
        self.init()
        return self

    def close(self, *args):
        [self._unregister(fd) for fd in self.readers]
        self.readers.clear()
        [self._unregister(fd) for fd in self.writers]
        self.writers.clear()
        for callback in self.on_close:
            callback(self)
    __exit__ = close

    def _repr_readers(self):
        return ['%s->%s->%r' % (_rcb(cb), repr_flag(READ | ERR), fd)
                for fd, cb in self.readers.items()]

    def _repr_writers(self):
        return ['%s->%s->%r' % (_rcb(cb), repr_flag(WRITE), fd)
                for fd, cb in self.writers.items()]

    def repr_active(self):
        return ', '.join(self._repr_readers() + self._repr_writers())

    def repr_events(self, events):
        return ', '.join(
            '%s->%s' % (_rcb(self._callback_for(fd, flags)), repr_flag(flags))
            for fd, flags in events)

    @cached_property
    def scheduler(self):
        return iter(self.timer)


class DummyLock(object):
    """Pretending to be a lock."""

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        pass
