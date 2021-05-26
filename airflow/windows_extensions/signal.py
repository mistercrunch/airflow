import _signal
import ctypes
from _signal import *
from functools import wraps as _wraps
from enum import IntEnum as _IntEnum
from threading import Timer
import inspect

"""
Contains extensions to emulate functions in the signal module that are only available on POXIS systems.
"""


# add missing signals
SIGALRM = 14
SIGVTALRM = 26
SIGPROF = 27
ITIMER_REAL = 0
added_signals = [SIGALRM, SIGVTALRM, SIGPROF]

# signal handlers
signal_handlers = dict()

# add timers
ITIMER_REAL = 0
ITIMER_VIRTUAL = 1
ITIMER_PROF = 2


class ITimerError(ValueError):

    def __init__(self):
        super().__init__('Invalid argument')


_globals = globals()

_IntEnum._convert(
        'Signals', __name__,
        lambda name:
            name.isupper()
            and (name.startswith('SIG') and not name.startswith('SIG_'))
            or name.startswith('CTRL_'))

_IntEnum._convert(
        'Handlers', __name__,
        lambda name: name in ('SIG_DFL', 'SIG_IGN'))

if 'pthread_sigmask' in _globals:
    _IntEnum._convert(
            'Sigmasks', __name__,
            lambda name: name in ('SIG_BLOCK', 'SIG_UNBLOCK', 'SIG_SETMASK'))


def _int_to_enum(value, enum_klass):
    """Convert a numeric value to an IntEnum member.
    If it's not a known member, return the numeric value itself.
    """
    try:
        return enum_klass(value)
    except ValueError:
        return value


def _enum_to_int(value):
    """Convert an IntEnum member to a numeric value.
    If it's not an IntEnum member return the value itself.
    """
    try:
        return int(value)
    except (ValueError, TypeError):
        return value


@_wraps(_signal.signal)
def signal(signalnum, handler):
    if signalnum in added_signals:
        old_handler = signal_handlers[signalnum] if signalnum in signal_handlers else None
        signal_handlers[signalnum] = handler
        return old_handler
    else:
        handler = _signal.signal(_enum_to_int(signalnum), _enum_to_int(handler))
        return _int_to_enum(handler, Handlers)


@_wraps(_signal.getsignal)
def getsignal(signalnum):
    if signalnum in added_signals:
        return signal_handlers[signalnum]
    else:
        handler = _signal.getsignal(signalnum)
        return _int_to_enum(handler, Handlers)


if 'pthread_sigmask' in _globals:
    @_wraps(_signal.pthread_sigmask)
    def pthread_sigmask(how, mask):
        sigs_set = _signal.pthread_sigmask(how, mask)
        return set(_int_to_enum(x, Signals) for x in sigs_set)
    pthread_sigmask.__doc__ = _signal.pthread_sigmask.__doc__


if 'sigpending' in _globals:
    @_wraps(_signal.sigpending)
    def sigpending():
        sigs = _signal.sigpending()
        return set(_int_to_enum(x, Signals) for x in sigs)


if 'sigwait' in _globals:
    @_wraps(_signal.sigwait)
    def sigwait(sigset):
        retsig = _signal.sigwait(sigset)
        return _int_to_enum(retsig, Signals)
    sigwait.__doc__ = _signal.sigwait


# timers
timers = {ITIMER_REAL: None, ITIMER_VIRTUAL: None, ITIMER_PROF: None}
timer_signals = {ITIMER_REAL: SIGALRM, ITIMER_VIRTUAL: SIGVTALRM, ITIMER_PROF: SIGPROF}

# reference to c-function for signaling
ucrtbase = ctypes.CDLL('ucrtbase')
c_raise = ucrtbase['raise']


def _handle_timer_(which, interval):
    signum = timer_signals[which].value
    
    if signum in added_signals and signum in signal_handlers and signal_handlers[signum] is not None:
        signal_handlers[signum](signum, inspect.currentframe())
    else:
        c_raise(signum)
    setitimer(which, interval, interval)

def setitimer(which, seconds, interval=0.0):
    if which not in timers.keys():
        raise ITimerError()

    # cleanup timer
    if timers[which] is not None:
        timers[which].cancel()

    # reset timer if interval and seconds are zero
    if seconds < 1e-12 and interval < 1e-12:
        timers[which] = None
        return

    # start timer
    timers[which] = Timer(seconds, lambda t=which, i=interval: _handle_timer_(t, i))
    timers[which].start()

del _globals, _wraps
