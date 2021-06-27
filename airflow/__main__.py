#!/usr/bin/env python
# PYTHON_ARGCOMPLETE_OK
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

"""Main executable module"""

import os
import sys

import argcomplete

from airflow.cli import cli_parser
from airflow.configuration import conf


def main():
    """Main executable function"""
    if conf.get("core", "security") == 'kerberos':
        os.environ['KRB5CCNAME'] = conf.get('kerberos', 'ccache')
        os.environ['KRB5_KTNAME'] = conf.get('kerberos', 'keytab')

    # if dags folder has to be set to configured value, make sure it is set properly (needed on Dask-Workers)
    if 'force_configured_dags_folder' in conf['core'] and conf.get('core', 'force_configured_dags_folder'):
        configured_dag_folder = conf.get('core', 'dags_folder')
        cmd_args = sys.argv[1:]
        for i in range(len(cmd_args)):
            if cmd_args[i] == '--subdir':
                dag_filename = os.path.split(cmd_args[i+1])[1]
                sys.argv[i+2] = os.path.join(configured_dag_folder, dag_filename)

    parser = cli_parser.get_parser()
    argcomplete.autocomplete(parser)
    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
