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

from builtins import str

import requests
import tenacity

from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException


class HttpHook(BaseHook):
    """
    Interact with HTTP servers.
    """

    def __init__(
        self,
        method='POST',
        http_conn_id='http_default'
    ):
        self.http_conn_id = http_conn_id
        self.method = method
        self.base_url = None
        self._retry_obj = None

    # headers may be passed through directly or in the "extra" field in the connection
    # definition
    def get_conn(self, headers=None):
        """
        Returns http session for use with requests
        """
        conn = self.get_connection(self.http_conn_id)
        session = requests.Session()

        if "://" in conn.host:
            self.base_url = conn.host
        else:
            # schema defaults to HTTP
            schema = conn.schema if conn.schema else "http"
            self.base_url = schema + "://" + conn.host

        if conn.port:
            self.base_url = self.base_url + ":" + str(conn.port) + "/"
        if conn.login:
            session.auth = (conn.login, conn.password)
        if conn.extra:
            session.headers.update(conn.extra_dejson)
        if headers:
            session.headers.update(headers)

        return session

    def run(self, endpoint, data=None, headers=None, extra_options=None):
        """
        Performs the request
        """
        extra_options = extra_options or {}

        session = self.get_conn(headers)

        url = self.base_url + endpoint
        req = None
        if self.method == 'GET':
            # GET uses params
            req = requests.Request(self.method,
                                   url,
                                   params=data,
                                   headers=headers)
        elif self.method == 'HEAD':
            # HEAD doesn't use params
            req = requests.Request(self.method,
                                   url,
                                   headers=headers)
        else:
            # Others use data
            req = requests.Request(self.method,
                                   url,
                                   data=data,
                                   headers=headers)

        prepped_request = session.prepare_request(req)
        self.log.info("Sending '%s' to url: %s", self.method, url)
        return self.run_and_check(session, prepped_request, extra_options)

    def check_response(self, response):
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            self.log.error("HTTP error: %s", response.reason)
            if self.method not in ['GET', 'HEAD']:
                self.log.error(response.text)
            raise AirflowException(str(response.status_code) + ":" + response.reason)

    def run_and_check(self, session, prepped_request, extra_options):
        """
        Grabs extra options like timeout and actually runs the request,
        checking for the result
        """
        extra_options = extra_options or {}

        try:
            response = session.send(
                prepped_request,
                stream=extra_options.get("stream", False),
                verify=extra_options.get("verify", False),
                proxies=extra_options.get("proxies", {}),
                cert=extra_options.get("cert"),
                timeout=extra_options.get("timeout"),
                allow_redirects=extra_options.get("allow_redirects", True))

            if extra_options.get('check_response', True):
                self.check_response(response)
            return response

        except requests.exceptions.ConnectionError as ex:
            self.log.warn(str(ex) + ' Tenacity will retry to execute the operation')
            raise ex

    def run_with_advanced_retry(self, _retry_args, *args, **kwargs):
        """
        Runs Hook.run() with a Tenacity decorator attached to it. This is useful for
        connectors which might be disturbed by intermittent issues and should not
        instantly fail.
        :param _retry_args: Arguments which define the retry behaviour.
            See Tenacity documentation at https://github.com/jd/tenacity
        :type _retry_args: dict
        """
        self._retry_obj = tenacity.Retrying(
            **_retry_args
        )

        self._retry_obj(self.run, *args, **kwargs)
