# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# This code originates from
# https://github.com/vterron/lemon/commit/9ca6b4b1212228dbd4f69b88aaf88b12952d7d6f
# and it has been updated to work with python 3.4+
import multiprocessing
from queue import Full, Empty

from multiprocessing.queues import Queue


class SharedCounter(object):
    """ A synchronized shared counter.

    The locking done by multiprocessing.Value ensures that only a single
    process or thread may read or write the in-memory ctypes object. However,
    in order to do n += 1, Python performs a read followed by a write, so a
    second process may read the old value before the new one is written by the
    first process. The solution is to use a multiprocessing.Lock to guarantee
    the atomicity of the modifications to Value.

    This class comes almost entirely from Eli Bendersky's blog:
    http://eli.thegreenplace.net/2012/01/04/shared-counter-with-pythons-multiprocessing/

    """

    def __init__(self, n=0):
        self.count = multiprocessing.Value('i', n)

    def increment(self, n=1):
        """ Increment the counter by n (default = 1) """
        with self.count.get_lock():
            self.count.value += n

    @property
    def value(self):
        """ Return the value of the counter """
        return self.count.value


class SynchronizedQueue(Queue):
    """ A portable implementation of multiprocessing.Queue with reliable empty() and qsize()

    Because of multithreading / multiprocessing semantics, Queue.qsize() may
    raise the NotImplementedError exception on Unix platforms like Mac OS X
    where sem_getvalue() is not implemented. This subclass addresses this
    problem by using a synchronized shared counter (initialized to zero) and
    increasing / decreasing its value every time the put() and get() methods
    are called, respectively. This not only prevents NotImplementedError from
    being raised, but also allows us to implement a reliable version of both
    qsize() and empty().

    """

    def __init__(self, maxsize=-1, block=True, timeout=None):
        self.block = block
        self.timeout = timeout
        self.size = SharedCounter(0)
        super().__init__(maxsize, ctx=multiprocessing.get_context())

    def put(self, obj, block=True, timeout=None):
        try:
            super(SynchronizedQueue, self).put(obj, block=block, timeout=timeout)
            # Only increment size if succeeded to put the object
            self.size.increment(1)
        except Full:
            raise

    def get(self, block=True, timeout=None):
        try:
            result = super(SynchronizedQueue, self).get(block=block, timeout=timeout)
            # Only decrement size if succeeded to get the object
            self.size.increment(-1)
            return result
        except Empty:
            raise

    def qsize(self):
        """ Reliable qsize implementation of multiprocessing.Queue.qsize().

        Unlike in the original queue, size() is guaranteed to return the actual size of the queue - matching
        put/get requests executed. It does however that qsize() > 0 will make new data available
        instantly. It is possible that in this case, get_nowait() or get(block=False) will throw an
        an Empty exception.

        If you expect data to be returned, you should use get() or check for an Empty exception.


        """
        # Synchronize on the lock
        self.size.increment(0)
        return self.size.value

    def empty(self):
        """ Reliable implementation of multiprocessing.Queue.empty().

        Unlike in the original queue, empty() is guaranteed to return True
        only when successful put requests matched get requests.

        It does not mean however that empty() == False will make new data available
        instantly. It is possible that in this case that, get_nowait() or get(block=False) will throw
        an Empty exception.

        If you expect data to be returned, you should use get() or check for an Empty exception.

        """
        return not self.qsize()
