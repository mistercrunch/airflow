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

import unittest

import pendulum
import pytest

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults, latest_only


# Essentially similar to airflow.models.BaseOperator
class DummyClass:
    @apply_defaults
    def __init__(self, test_param, params=None, default_args=None):  # pylint: disable=unused-argument
        self.test_param = test_param


class DummySubClass(DummyClass):
    @apply_defaults
    def __init__(self, test_sub_param, **kwargs):
        super().__init__(**kwargs)
        self.test_sub_param = test_sub_param


class TestApplyDefault(unittest.TestCase):
    def test_apply(self):
        dummy = DummyClass(test_param=True)
        assert dummy.test_param

        with pytest.raises(AirflowException, match='Argument.*test_param.*required'):
            DummySubClass(test_sub_param=True)

    def test_default_args(self):
        default_args = {'test_param': True}
        dummy_class = DummyClass(default_args=default_args)  # pylint: disable=no-value-for-parameter
        assert dummy_class.test_param

        default_args = {'test_param': True, 'test_sub_param': True}
        dummy_subclass = DummySubClass(default_args=default_args)  # pylint: disable=no-value-for-parameter
        assert dummy_class.test_param
        assert dummy_subclass.test_sub_param

        default_args = {'test_param': True}
        dummy_subclass = DummySubClass(default_args=default_args, test_sub_param=True)
        assert dummy_class.test_param
        assert dummy_subclass.test_sub_param

        with pytest.raises(AirflowException, match='Argument.*test_sub_param.*required'):
            DummySubClass(default_args=default_args)  # pylint: disable=no-value-for-parameter

    def test_incorrect_default_args(self):
        default_args = {'test_param': True, 'extra_param': True}
        dummy_class = DummyClass(default_args=default_args)  # pylint: disable=no-value-for-parameter
        assert dummy_class.test_param

        default_args = {'random_params': True}
        with pytest.raises(AirflowException, match='Argument.*test_param.*required'):
            DummyClass(default_args=default_args)  # pylint: disable=no-value-for-parameter


class TestLatestOnly:
    def test_latest_only(self):
        class MyOp(BaseOperator):
            @latest_only
            def execute(self, context):
                return 'val'

        class MockDag:
            @staticmethod
            def following_schedule(execution_date):
                return execution_date.add(days=1)

        op = MyOp(task_id='hello')

        with pytest.raises(AirflowSkipException):
            execution_date = pendulum.now().add(days=-1)
            op.execute({'dag': MockDag(), 'execution_date': execution_date.add(hours=-36)})
        val = op.execute({'dag': MockDag(), 'execution_date': execution_date.add(hours=-12)})
        assert val == 'val'

    def test_latest_only_disable(self):
        """Latest only behavior should be disableable"""

        class MyOp(BaseOperator):
            def __init__(self, latest_only=False, **kwargs):
                super().__init__(**kwargs)
                self.latest_only = latest_only

            @latest_only
            def execute(self, context):
                return 'val'

        class MockDag:
            @staticmethod
            def following_schedule(execution_date):
                return execution_date.add(days=1)

        execution_date = pendulum.now().add(days=-1)
        context = {'dag': MockDag(), 'execution_date': execution_date.add(hours=-36)}
        with pytest.raises(AirflowSkipException):
            op = MyOp(task_id='hello', latest_only=True)
            op.execute(context)
        op = MyOp(task_id='hello', latest_only=False)
        val = op.execute(context)
        assert val == 'val'
