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

import contextlib
from functools import wraps
from inspect import signature
from typing import Callable, TypeVar

from airflow import settings


@contextlib.contextmanager
def create_session():
    """Contextmanager that will create and teardown a session."""
    session = settings.Session()
    try:
        yield session
        # Only Commit if a new or a modified object exists in the session
        if not session._is_clean():  # pylint: disable=protected-access
            session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


RT = TypeVar("RT")  # pylint: disable=invalid-name


def provide_session(func: Callable[..., RT]) -> Callable[..., RT]:
    """
    Function decorator that provides a session if it isn't provided.
    If you want to reuse a session or run the function as part of a
    database transaction, you pass it to the function, if not this wrapper
    will create one and close it for you.
    """
    func_params = signature(func).parameters
    try:
        # func_params is an ordered dict -- this is the "recommended" way of getting the position
        session_args_idx = tuple(func_params).index("session")
    except ValueError:
        raise ValueError(f"Function {func.__qualname__} has no `session` argument") from None
    # We don't need this anymore -- ensure we don't keep a reference to it by mistake
    del func_params

    @wraps(func)
    def wrapper(*args, **kwargs) -> RT:
        if "session" in kwargs or session_args_idx < len(args):
            return func(*args, **kwargs)
        else:
            with create_session() as session:
                return func(*args, session=session, **kwargs)

    return wrapper
