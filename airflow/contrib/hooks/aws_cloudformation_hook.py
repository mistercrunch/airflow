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

"""
This module contains AWS CloudFormation Hook
"""
from airflow.contrib.hooks.aws_hook import AwsHook


class CloudFormationHook(AwsHook):
    """
    Interact with AWS CloudFormation.
    """

    def __init__(self, region_name=None, *args, **kwargs):
        self.region_name = region_name
        self.conn = None
        super().__init__(*args, **kwargs)

    def get_conn(self):
        self.conn = self.get_client_type('cloudformation', self.region_name)
        return self.conn
