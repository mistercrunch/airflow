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

import unittest

from airflow import DAG
from airflow.contrib.operators.s3_to_snowflake_operator import S3ToSnowflakeOperator
from tests.compat import mock
from airflow.utils.tests import assertEqualIgnoreMultipleSpaces


class TestS3ToRedshiftOperator(unittest.TestCase):
    @mock.patch("airflow.contrib.hooks.snowflake_hook.SnowflakeHook.run")
    def test_execute(self, mock_run):
        s3_keys = ['1.csv', '2.csv']
        table = 'table'
        stage = 'stage'
        file_format = 'file_format'
        schema = 'schema'

        t = S3ToSnowflakeOperator(
            s3_keys=s3_keys,
            table=table,
            stage=stage,
            file_format=file_format,
            schema=schema,
            columns_array=None,
        ).execute(None)

        base_sql = """
                FROM @{stage}/
                files={files}
                file_format={file_format}
            """.format(
            stage=stage,
            files=s3_keys,
            file_format=file_format
        )

        copy_query = """
                COPY INTO {schema}.{table} {base_sql}
            """.format(
            schema=schema,
            table=table,
            base_sql=base_sql
        )

        assert mock_run.call_count == 1
        assertEqualIgnoreMultipleSpaces(self, mock_run.call_args[0][0], copy_query)
