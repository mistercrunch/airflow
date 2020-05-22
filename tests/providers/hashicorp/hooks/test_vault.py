# pylint: disable=no-member
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

from unittest import mock
from unittest.case import TestCase

from hvac.exceptions import InvalidPath, VaultError
from mock import PropertyMock, mock_open, patch

from airflow.providers.hashicorp.hooks.vault import VaultHook, _VaultClient


# noinspection DuplicatedCode,PyUnresolvedReferences
class TestVaultHook(TestCase):

    @staticmethod
    def get_mock_connection(schema="http", host="localhost", port=8180, user="user", password="pass"):
        mock_connection = mock.MagicMock()
        type(mock_connection).schema = PropertyMock(return_value=schema)
        type(mock_connection).host = PropertyMock(return_value=host)
        type(mock_connection).port = PropertyMock(return_value=port)
        type(mock_connection).login = PropertyMock(return_value=user)
        type(mock_connection).password = PropertyMock(return_value=password)
        return mock_connection

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_version_not_int(self, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        connection_dict = {
            "auth_type": "userpass",
            "kv_engine_version": "text"
        }

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "vault_conn_id": "vault_conn_id",
        }
        with self.assertRaisesRegex(VaultError, 'The version is not an int: text'):
            VaultHook(**kwargs)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_version_as_string(self, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        connection_dict = {
            "auth_type": "userpass",
            "kv_engine_version": "2"
        }

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "vault_conn_id": "vault_conn_id",
        }
        test_hook = VaultHook(**kwargs)
        self.assertEqual(2, test_hook.vault_client.kv_engine_version)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_custom_mount_point_init_params(self, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        connection_dict = {
            "auth_type": "userpass",
        }

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "vault_conn_id": "vault_conn_id",
            "mount_point": "custom"
        }
        test_hook = VaultHook(**kwargs)
        self.assertEqual("custom", test_hook.vault_client.mount_point)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_custom_mount_point_dejson(self, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        connection_dict = {
            "auth_type": "userpass",
            "mount_point": "custom"
        }

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "vault_conn_id": "vault_conn_id",
        }
        test_hook = VaultHook(**kwargs)
        self.assertEqual("custom", test_hook.vault_client.mount_point)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_version_one_init(self, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        connection_dict = {
            "auth_type": "userpass",
            "kv_engine_version": 1
        }

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "vault_conn_id": "vault_conn_id",
        }
        test_hook = VaultHook(**kwargs)
        self.assertEqual(1, test_hook.vault_client.kv_engine_version)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_version_one_dejson(self, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        connection_dict = {
            "auth_type": "userpass",
        }

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "kv_engine_version": 1,
            "vault_conn_id": "vault_conn_id",
        }
        test_hook = VaultHook(**kwargs)
        self.assertEqual(1, test_hook.vault_client.kv_engine_version)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_approle_init_params(self, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        connection_dict = {}

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "vault_conn_id": "vault_conn_id",
            "auth_type": "approle",
            "role_id": "role",
            "kv_engine_version": 2
        }

        test_hook = VaultHook(**kwargs)
        mock_get_connection.assert_called_with("vault_conn_id")
        test_client = test_hook.get_conn()
        mock_hvac.Client.assert_called_with(url='http://localhost:8180')
        test_client.auth_approle.assert_called_with(role_id="role", secret_id="pass")
        test_client.is_authenticated.assert_called_with()
        self.assertEqual(2, test_hook.vault_client.kv_engine_version)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_approle_dejson(self, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        connection_dict = {
            "auth_type": "approle",
            'role_id': "role",
        }

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "vault_conn_id": "vault_conn_id",
        }

        test_hook = VaultHook(**kwargs)
        mock_get_connection.assert_called_with("vault_conn_id")
        test_client = test_hook.get_conn()
        mock_hvac.Client.assert_called_with(url='http://localhost:8180')
        test_client.auth_approle.assert_called_with(role_id="role", secret_id="pass")
        test_client.is_authenticated.assert_called_with()
        self.assertEqual(2, test_hook.vault_client.kv_engine_version)

    @mock.patch("airflow.providers.google.cloud.utils.credentials_provider._get_scopes")
    @mock.patch("airflow.providers.google.cloud.utils.credentials_provider.get_credentials_and_project_id")
    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_gcp_init_params(self, mock_hvac, mock_get_connection,
                             mock_get_credentials, mock_get_scopes):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection
        mock_get_scopes.return_value = ['scope1', 'scope2']
        mock_get_credentials.return_value = ("credentials", "project_id")

        connection_dict = {}

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "vault_conn_id": "vault_conn_id",
            "auth_type": "gcp",
            "gcp_key_path": "path.json",
            "gcp_scopes": "scope1,scope2",
        }

        test_hook = VaultHook(**kwargs)
        test_client = test_hook.get_conn()
        mock_get_connection.assert_called_with("vault_conn_id")
        mock_get_scopes.assert_called_with("scope1,scope2")
        mock_get_credentials.assert_called_with(
            key_path="path.json",
            scopes=['scope1', 'scope2']
        )
        mock_hvac.Client.assert_called_with(url='http://localhost:8180')
        test_client.auth.gcp.configure.assert_called_with(
            credentials="credentials",
        )
        test_client.is_authenticated.assert_called_with()
        self.assertEqual(2, test_hook.vault_client.kv_engine_version)

    @mock.patch("airflow.providers.google.cloud.utils.credentials_provider._get_scopes")
    @mock.patch("airflow.providers.google.cloud.utils.credentials_provider.get_credentials_and_project_id")
    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_gcp_dejson(self, mock_hvac, mock_get_connection,
                        mock_get_credentials, mock_get_scopes):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection
        mock_get_scopes.return_value = ['scope1', 'scope2']
        mock_get_credentials.return_value = ("credentials", "project_id")

        connection_dict = {
            "auth_type": "gcp",
            "gcp_key_path": "path.json",
            "gcp_scopes": "scope1,scope2",
        }

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "vault_conn_id": "vault_conn_id",
        }

        test_hook = VaultHook(**kwargs)
        test_client = test_hook.get_conn()
        mock_get_connection.assert_called_with("vault_conn_id")
        mock_get_scopes.assert_called_with("scope1,scope2")
        mock_get_credentials.assert_called_with(
            key_path="path.json",
            scopes=['scope1', 'scope2']
        )
        mock_hvac.Client.assert_called_with(url='http://localhost:8180')
        test_client.auth.gcp.configure.assert_called_with(
            credentials="credentials",
        )
        test_client.is_authenticated.assert_called_with()
        self.assertEqual(2, test_hook.vault_client.kv_engine_version)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_github_init_params(self, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        connection_dict = {}

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "auth_type": "github",
            "vault_conn_id": "vault_conn_id",
            "token": "s.7AU0I51yv1Q1lxOIg1F3ZRAS",
        }

        test_hook = VaultHook(**kwargs)
        mock_get_connection.assert_called_with("vault_conn_id")
        test_client = test_hook.get_conn()
        mock_hvac.Client.assert_called_with(url='http://localhost:8180')
        test_client.auth.github.login.assert_called_with(
            token="s.7AU0I51yv1Q1lxOIg1F3ZRAS")
        test_client.is_authenticated.assert_called_with()
        self.assertEqual(2, test_hook.vault_client.kv_engine_version)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_github_dejson(self, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        connection_dict = {
            "auth_type": "github",
            "token": "s.7AU0I51yv1Q1lxOIg1F3ZRAS",
        }

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "vault_conn_id": "vault_conn_id",
        }

        test_hook = VaultHook(**kwargs)
        mock_get_connection.assert_called_with("vault_conn_id")
        test_client = test_hook.get_conn()
        mock_hvac.Client.assert_called_with(url='http://localhost:8180')
        test_client.auth.github.login.assert_called_with(
            token="s.7AU0I51yv1Q1lxOIg1F3ZRAS")
        test_client.is_authenticated.assert_called_with()
        self.assertEqual(2, test_hook.vault_client.kv_engine_version)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_kubernetes_default_path(self, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        connection_dict = {}

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "auth_type": "kubernetes",
            "kubernetes_role": "kube_role",
            "vault_conn_id": "vault_conn_id",
        }

        with patch("builtins.open", mock_open(read_data="data")) as mock_file:
            test_hook = VaultHook(**kwargs)
            test_client = test_hook.get_conn()
        mock_get_connection.assert_called_with("vault_conn_id")
        mock_file.assert_called_with("/var/run/secrets/kubernetes.io/serviceaccount/token")
        mock_hvac.Client.assert_called_with(url='http://localhost:8180')
        test_client.auth_kubernetes.assert_called_with(
            role="kube_role", jwt="data")
        test_client.is_authenticated.assert_called_with()
        self.assertEqual(2, test_hook.vault_client.kv_engine_version)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_kubernetes_init_params(self, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        connection_dict = {
            "kubernetes_role": "kube_role",
            "kubernetes_jwt_path": "path",
        }

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "auth_type": "kubernetes",
            "vault_conn_id": "vault_conn_id",
        }
        with patch("builtins.open", mock_open(read_data="data")) as mock_file:
            test_hook = VaultHook(**kwargs)
            test_client = test_hook.get_conn()
        mock_get_connection.assert_called_with("vault_conn_id")
        mock_file.assert_called_with("path")
        mock_hvac.Client.assert_called_with(url='http://localhost:8180')
        test_client.auth_kubernetes.assert_called_with(
            role="kube_role", jwt="data")
        test_client.is_authenticated.assert_called_with()
        self.assertEqual(2, test_hook.vault_client.kv_engine_version)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_kubernetes_dejson(self, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        connection_dict = {}

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "kubernetes_role": "kube_role",
            "kubernetes_jwt_path": "path",
            "auth_type": "kubernetes",
            "vault_conn_id": "vault_conn_id",
        }
        with patch("builtins.open", mock_open(read_data="data")) as mock_file:
            test_hook = VaultHook(**kwargs)
            test_client = test_hook.get_conn()
        mock_get_connection.assert_called_with("vault_conn_id")
        mock_file.assert_called_with("path")
        mock_hvac.Client.assert_called_with(url='http://localhost:8180')
        test_client.auth_kubernetes.assert_called_with(
            role="kube_role", jwt="data")
        test_client.is_authenticated.assert_called_with()
        self.assertEqual(2, test_hook.vault_client.kv_engine_version)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_ldap_init_params(self, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        connection_dict = {}

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "auth_type": "ldap",
            "vault_conn_id": "vault_conn_id",
        }

        test_hook = VaultHook(**kwargs)
        mock_get_connection.assert_called_with("vault_conn_id")
        test_client = test_hook.get_conn()
        mock_hvac.Client.assert_called_with(url='http://localhost:8180')
        test_client.auth.ldap.login.assert_called_with(
            username="user", password="pass")
        test_client.is_authenticated.assert_called_with()
        self.assertEqual(2, test_hook.vault_client.kv_engine_version)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_ldap_dejson(self, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        connection_dict = {
            "auth_type": "ldap",
        }

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "vault_conn_id": "vault_conn_id",
        }

        test_hook = VaultHook(**kwargs)
        mock_get_connection.assert_called_with("vault_conn_id")
        test_client = test_hook.get_conn()
        mock_hvac.Client.assert_called_with(url='http://localhost:8180')
        test_client.auth.ldap.login.assert_called_with(
            username="user", password="pass")
        test_client.is_authenticated.assert_called_with()
        self.assertEqual(2, test_hook.vault_client.kv_engine_version)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_token_init_params(self, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection
        connection_dict = {}
        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "vault_conn_id": "vault_conn_id",
            "auth_type": "token",
            "token": "s.7AU0I51yv1Q1lxOIg1F3ZRAS",
            "kv_engine_version": 2
        }

        test_hook = VaultHook(**kwargs)
        mock_get_connection.assert_called_with("vault_conn_id")
        test_client = test_hook.get_conn()
        mock_hvac.Client.assert_called_with(url='http://localhost:8180')
        test_client.is_authenticated.assert_called_with()
        self.assertEqual("s.7AU0I51yv1Q1lxOIg1F3ZRAS", test_client.token)
        self.assertEqual(2, test_hook.vault_client.kv_engine_version)
        self.assertEqual("secret", test_hook.vault_client.mount_point)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_token_dejson(self, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        connection_dict = {
            "auth_type": "token",
            "token": "s.7AU0I51yv1Q1lxOIg1F3ZRAS",
        }

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "vault_conn_id": "vault_conn_id",
        }

        test_hook = VaultHook(**kwargs)
        mock_get_connection.assert_called_with("vault_conn_id")
        test_client = test_hook.get_conn()
        mock_hvac.Client.assert_called_with(url='http://localhost:8180')
        test_client.is_authenticated.assert_called_with()
        self.assertEqual("s.7AU0I51yv1Q1lxOIg1F3ZRAS", test_client.token)
        self.assertEqual(2, test_hook.vault_client.kv_engine_version)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_userpass_init_params(self, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        connection_dict = {}

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "vault_conn_id": "vault_conn_id",
            "auth_type": "userpass",
            "kv_engine_version": 2
        }

        test_hook = VaultHook(**kwargs)
        mock_get_connection.assert_called_with("vault_conn_id")
        test_client = test_hook.get_conn()
        mock_hvac.Client.assert_called_with(url='http://localhost:8180')
        test_client.auth_userpass.assert_called_with(
            username="user", password="pass")
        test_client.is_authenticated.assert_called_with()
        self.assertEqual(2, test_hook.vault_client.kv_engine_version)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_userpass_dejson(self, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        connection_dict = {
            "auth_type": "userpass",
        }

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "vault_conn_id": "vault_conn_id",
        }

        test_hook = VaultHook(**kwargs)
        mock_get_connection.assert_called_with("vault_conn_id")
        test_client = test_hook.get_conn()
        mock_hvac.Client.assert_called_with(url='http://localhost:8180')
        test_client.auth_userpass.assert_called_with(
            username="user", password="pass")
        test_client.is_authenticated.assert_called_with()
        self.assertEqual(2, test_hook.vault_client.kv_engine_version)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_get_existing_key_v2(self, mock_hvac, mock_get_connection):
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

        connection_dict = {}

        mock_client.secrets.kv.v2.read_secret_version.return_value = {
            'request_id': '94011e25-f8dc-ec29-221b-1f9c1d9ad2ae',
            'lease_id': '',
            'renewable': False,
            'lease_duration': 0,
            'data': {
                'data': {'secret_key': 'secret_value'},
                'metadata': {'created_time': '2020-03-16T21:01:43.331126Z',
                             'deletion_time': '',
                             'destroyed': False,
                             'version': 1}},
            'wrap_info': None,
            'warnings': None,
            'auth': None
        }

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "vault_conn_id": "vault_conn_id",
            "auth_type": "token",
            "token": "s.7AU0I51yv1Q1lxOIg1F3ZRAS",
            "kv_engine_version": 2
        }

        test_hook = VaultHook(**kwargs)
        secret = test_hook.get_secret(secret_path="missing")
        self.assertEqual({'secret_key': 'secret_value'}, secret)
        mock_client.secrets.kv.v2.read_secret_version.assert_called_once_with(
            mount_point='secret', path='missing', version=None)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_get_existing_key_v2_version(self, mock_hvac, mock_get_connection):
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

        connection_dict = {}

        mock_client.secrets.kv.v2.read_secret_version.return_value = {
            'request_id': '94011e25-f8dc-ec29-221b-1f9c1d9ad2ae',
            'lease_id': '',
            'renewable': False,
            'lease_duration': 0,
            'data': {
                'data': {'secret_key': 'secret_value'},
                'metadata': {'created_time': '2020-03-16T21:01:43.331126Z',
                             'deletion_time': '',
                             'destroyed': False,
                             'version': 1}},
            'wrap_info': None,
            'warnings': None,
            'auth': None
        }

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "vault_conn_id": "vault_conn_id",
            "auth_type": "token",
            "token": "s.7AU0I51yv1Q1lxOIg1F3ZRAS",
            "kv_engine_version": 2
        }

        test_hook = VaultHook(**kwargs)
        secret = test_hook.get_secret(secret_path="missing", secret_version=1)
        self.assertEqual({'secret_key': 'secret_value'}, secret)
        mock_client.secrets.kv.v2.read_secret_version.assert_called_once_with(
            mount_point='secret', path='missing', version=1)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_get_existing_key_v1(self, mock_hvac, mock_get_connection):
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

        connection_dict = {}

        mock_client.secrets.kv.v1.read_secret.return_value = {
            'request_id': '182d0673-618c-9889-4cba-4e1f4cfe4b4b',
            'lease_id': '',
            'renewable': False,
            'lease_duration': 2764800,
            'data': {'value': 'world'},
            'wrap_info': None,
            'warnings': None,
            'auth': None}

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "vault_conn_id": "vault_conn_id",
            "auth_type": "token",
            "token": "s.7AU0I51yv1Q1lxOIg1F3ZRAS",
            "kv_engine_version": 1
        }

        test_hook = VaultHook(**kwargs)
        secret = test_hook.get_secret(secret_path="missing")
        self.assertEqual({'value': 'world'}, secret)
        mock_client.secrets.kv.v1.read_secret.assert_called_once_with(
            mount_point='secret', path='missing')

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_get_secret_metadata_v2(self, mock_hvac, mock_get_connection):
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

        connection_dict = {}

        mock_client.secrets.kv.v2.read_secret_metadata.return_value = {
            'request_id': '94011e25-f8dc-ec29-221b-1f9c1d9ad2ae',
            'lease_id': '',
            'renewable': False,
            'lease_duration': 0,
            'metadata': [
                {'created_time': '2020-03-16T21:01:43.331126Z',
                 'deletion_time': '',
                 'destroyed': False,
                 'version': 1},
                {'created_time': '2020-03-16T21:01:43.331126Z',
                 'deletion_time': '',
                 'destroyed': False,
                 'version': 2},
            ]
        }

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "vault_conn_id": "vault_conn_id",
            "auth_type": "token",
            "token": "s.7AU0I51yv1Q1lxOIg1F3ZRAS",
            "kv_engine_version": 2
        }

        test_hook = VaultHook(**kwargs)
        metadata = test_hook.get_secret_metadata(secret_path="missing")
        self.assertEqual(
            {
                'request_id': '94011e25-f8dc-ec29-221b-1f9c1d9ad2ae',
                'lease_id': '',
                'renewable': False,
                'lease_duration': 0,
                'metadata': [
                    {'created_time': '2020-03-16T21:01:43.331126Z',
                     'deletion_time': '',
                     'destroyed': False,
                     'version': 1},
                    {'created_time': '2020-03-16T21:01:43.331126Z',
                     'deletion_time': '',
                     'destroyed': False,
                     'version': 2},
                ]
            }, metadata)
        mock_client.secrets.kv.v2.read_secret_metadata.assert_called_once_with(
            mount_point='secret', path='missing')

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_get_secret_including_metadata_v2(self, mock_hvac, mock_get_connection):
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

        connection_dict = {}

        mock_client.secrets.kv.v2.read_secret_version.return_value = {
            'request_id': '94011e25-f8dc-ec29-221b-1f9c1d9ad2ae',
            'lease_id': '',
            'renewable': False,
            'lease_duration': 0,
            'data': {
                'data': {'secret_key': 'secret_value'},
                'metadata': {'created_time': '2020-03-16T21:01:43.331126Z',
                             'deletion_time': '',
                             'destroyed': False,
                             'version': 1}},
            'wrap_info': None,
            'warnings': None,
            'auth': None
        }

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "vault_conn_id": "vault_conn_id",
            "auth_type": "token",
            "token": "s.7AU0I51yv1Q1lxOIg1F3ZRAS",
            "kv_engine_version": 2
        }

        test_hook = VaultHook(**kwargs)
        metadata = test_hook.get_secret_including_metadata(secret_path="missing")
        self.assertEqual(
            {
                'request_id': '94011e25-f8dc-ec29-221b-1f9c1d9ad2ae',
                'lease_id': '',
                'renewable': False,
                'lease_duration': 0,
                'data': {
                    'data': {'secret_key': 'secret_value'},
                    'metadata': {'created_time': '2020-03-16T21:01:43.331126Z',
                                 'deletion_time': '',
                                 'destroyed': False,
                                 'version': 1}},
                'wrap_info': None,
                'warnings': None,
                'auth': None
            }, metadata)
        mock_client.secrets.kv.v2.read_secret_version.assert_called_once_with(
            mount_point='secret', path='missing', version=None)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_create_or_update_secret_v2(self, mock_hvac, mock_get_connection):
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

        connection_dict = {}

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "vault_conn_id": "vault_conn_id",
            "auth_type": "token",
            "token": "s.7AU0I51yv1Q1lxOIg1F3ZRAS",
            "kv_engine_version": 2
        }

        test_hook = VaultHook(**kwargs)
        test_hook.create_or_update_secret(
            secret_path="path",
            secret={'key': 'value'}
        )
        mock_client.secrets.kv.v2.create_or_update_secret.assert_called_once_with(
            mount_point='secret', secret_path='path', secret={'key': 'value'}, cas=None)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_create_or_update_secret_v2_cas(self, mock_hvac, mock_get_connection):
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

        connection_dict = {}

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "vault_conn_id": "vault_conn_id",
            "auth_type": "token",
            "token": "s.7AU0I51yv1Q1lxOIg1F3ZRAS",
            "kv_engine_version": 2
        }

        test_hook = VaultHook(**kwargs)
        test_hook.create_or_update_secret(
            secret_path="path",
            secret={'key': 'value'},
            cas=10
        )
        mock_client.secrets.kv.v2.create_or_update_secret.assert_called_once_with(
            mount_point='secret', secret_path='path', secret={'key': 'value'}, cas=10)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_create_or_update_secret_v1(self, mock_hvac, mock_get_connection):
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

        connection_dict = {}

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "vault_conn_id": "vault_conn_id",
            "auth_type": "token",
            "token": "s.7AU0I51yv1Q1lxOIg1F3ZRAS",
            "kv_engine_version": 1
        }

        test_hook = VaultHook(**kwargs)
        test_hook.create_or_update_secret(
            secret_path="path",
            secret={'key': 'value'}
        )
        mock_client.secrets.kv.v1.create_or_update_secret.assert_called_once_with(
            mount_point='secret', secret_path='path', secret={'key': 'value'}, method=None)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_create_or_update_secret_v1_post(self, mock_hvac, mock_get_connection):
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

        connection_dict = {}

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "vault_conn_id": "vault_conn_id",
            "auth_type": "token",
            "token": "s.7AU0I51yv1Q1lxOIg1F3ZRAS",
            "kv_engine_version": 1
        }

        test_hook = VaultHook(**kwargs)
        test_hook.create_or_update_secret(
            secret_path="path",
            secret={'key': 'value'},
            method="post"
        )
        mock_client.secrets.kv.v1.create_or_update_secret.assert_called_once_with(
            mount_point='secret', secret_path='path', secret={'key': 'value'}, method="post")


class TestVaultClient(TestCase):

    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_version_wrong(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        with self.assertRaisesRegex(VaultError, 'The version is not supported: 4'):
            _VaultClient(auth_type="approle", kv_engine_version=4)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_custom_mount_point(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        vault_client = _VaultClient(auth_type="userpass", mount_point="custom")
        self.assertEqual("custom", vault_client.mount_point)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_version_one_init(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

        vault_client = _VaultClient(auth_type="userpass", kv_engine_version=1)
        self.assertEqual(1, vault_client.kv_engine_version)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_approle(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        vault_client = _VaultClient(auth_type="approle", role_id="role", url="http://localhost:8180",
                                    secret_id="pass")
        client = vault_client.client
        mock_hvac.Client.assert_called_with(url='http://localhost:8180')
        client.auth_approle.assert_called_with(role_id="role", secret_id="pass")
        client.is_authenticated.assert_called_with()
        self.assertEqual(2, vault_client.kv_engine_version)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_approle_missing_role(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        with self.assertRaisesRegex(VaultError, "requires 'role_id'"):
            _VaultClient(
                auth_type="approle",
                url="http://localhost:8180",
                secret_id="pass")

    @mock.patch("airflow.providers.google.cloud.utils.credentials_provider._get_scopes")
    @mock.patch("airflow.providers.google.cloud.utils.credentials_provider.get_credentials_and_project_id")
    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_gcp(self, mock_hvac, mock_get_credentials, mock_get_scopes):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_get_scopes.return_value = ['scope1', 'scope2']
        mock_get_credentials.return_value = ("credentials", "project_id")
        vault_client = _VaultClient(auth_type="gcp", gcp_key_path="path.json", gcp_scopes="scope1,scope2",
                                    url="http://localhost:8180")
        client = vault_client.client
        mock_hvac.Client.assert_called_with(url='http://localhost:8180')
        mock_get_scopes.assert_called_with("scope1,scope2")
        mock_get_credentials.assert_called_with(
            key_path="path.json",
            scopes=['scope1', 'scope2']
        )
        mock_hvac.Client.assert_called_with(url='http://localhost:8180')
        client.auth.gcp.configure.assert_called_with(
            credentials="credentials",
        )
        client.is_authenticated.assert_called_with()
        self.assertEqual(2, vault_client.kv_engine_version)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_github(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        vault_client = _VaultClient(auth_type="github",
                                    token="s.7AU0I51yv1Q1lxOIg1F3ZRAS",
                                    url="http://localhost:8180")
        client = vault_client.client
        mock_hvac.Client.assert_called_with(url='http://localhost:8180')
        client.auth.github.login.assert_called_with(
            token="s.7AU0I51yv1Q1lxOIg1F3ZRAS")
        client.is_authenticated.assert_called_with()
        self.assertEqual(2, vault_client.kv_engine_version)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_github_missing_token(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        with self.assertRaisesRegex(VaultError, "'github' authentication type requires 'token'"):
            _VaultClient(auth_type="github", url="http://localhost:8180")

    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_kubernetes_default_path(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        vault_client = _VaultClient(auth_type="kubernetes",
                                    kubernetes_role="kube_role",
                                    url="http://localhost:8180")
        with patch("builtins.open", mock_open(read_data="data")) as mock_file:
            client = vault_client.client
        mock_file.assert_called_with("/var/run/secrets/kubernetes.io/serviceaccount/token")
        mock_hvac.Client.assert_called_with(url='http://localhost:8180')
        client.auth_kubernetes.assert_called_with(
            role="kube_role", jwt="data")
        client.is_authenticated.assert_called_with()
        self.assertEqual(2, vault_client.kv_engine_version)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_kubernetes(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        vault_client = _VaultClient(auth_type="kubernetes",
                                    kubernetes_role="kube_role",
                                    kubernetes_jwt_path="path",
                                    url="http://localhost:8180")
        with patch("builtins.open", mock_open(read_data="data")) as mock_file:
            client = vault_client.client
        mock_file.assert_called_with("path")
        mock_hvac.Client.assert_called_with(url='http://localhost:8180')
        client.auth_kubernetes.assert_called_with(
            role="kube_role", jwt="data")
        client.is_authenticated.assert_called_with()
        self.assertEqual(2, vault_client.kv_engine_version)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_kubernetes_missing_role(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        with self.assertRaisesRegex(VaultError, "requires 'kubernetes_role'"):
            _VaultClient(auth_type="kubernetes",
                         kubernetes_jwt_path="path",
                         url="http://localhost:8180")

    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_kubernetes_kubernetes_jwt_path_none(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        with self.assertRaisesRegex(VaultError, "requires 'kubernetes_jwt_path'"):
            _VaultClient(auth_type="kubernetes",
                         kubernetes_role='kube_role',
                         kubernetes_jwt_path=None,
                         url="http://localhost:8180")

    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_ldap(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        vault_client = _VaultClient(auth_type="ldap",
                                    username="user",
                                    password="pass",
                                    url="http://localhost:8180")
        client = vault_client.client
        mock_hvac.Client.assert_called_with(url='http://localhost:8180')
        client.auth.ldap.login.assert_called_with(
            username="user", password="pass")
        client.is_authenticated.assert_called_with()
        self.assertEqual(2, vault_client.kv_engine_version)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_token_missing_token(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        with self.assertRaisesRegex(VaultError, "'token' authentication type requires 'token'"):
            _VaultClient(auth_type="token", url="http://localhost:8180")

    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_token(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        vault_client = _VaultClient(auth_type="token",
                                    token="s.7AU0I51yv1Q1lxOIg1F3ZRAS", url="http://localhost:8180")
        client = vault_client.client
        mock_hvac.Client.assert_called_with(url='http://localhost:8180')
        client.is_authenticated.assert_called_with()
        self.assertEqual("s.7AU0I51yv1Q1lxOIg1F3ZRAS", client.token)
        self.assertEqual(2, vault_client.kv_engine_version)
        self.assertEqual("secret", vault_client.mount_point)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_default_auth_type(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        vault_client = _VaultClient(token="s.7AU0I51yv1Q1lxOIg1F3ZRAS", url="http://localhost:8180")
        client = vault_client.client
        mock_hvac.Client.assert_called_with(url='http://localhost:8180')
        client.is_authenticated.assert_called_with()
        self.assertEqual("s.7AU0I51yv1Q1lxOIg1F3ZRAS", client.token)
        self.assertEqual("token", vault_client.auth_type)
        self.assertEqual(2, vault_client.kv_engine_version)
        self.assertEqual("secret", vault_client.mount_point)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_userpass(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        vault_client = _VaultClient(auth_type="userpass",
                                    username="user", password="pass", url="http://localhost:8180")
        client = vault_client.client
        mock_hvac.Client.assert_called_with(url='http://localhost:8180')
        client.auth_userpass.assert_called_with(
            username="user", password="pass")
        client.is_authenticated.assert_called_with()
        self.assertEqual(2, vault_client.kv_engine_version)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_get_non_existing_key_v2(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        # Response does not contain the requested key
        mock_client.secrets.kv.v2.read_secret_version.side_effect = InvalidPath()
        vault_client = _VaultClient(auth_type="token", token="s.7AU0I51yv1Q1lxOIg1F3ZRAS",
                                    url="http://localhost:8180")
        secret = vault_client.get_secret(secret_path="missing")
        self.assertIsNone(secret)
        mock_client.secrets.kv.v2.read_secret_version.assert_called_once_with(
            mount_point='secret', path='missing', version=None)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_get_non_existing_key_v2_different_auth(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        # Response does not contain the requested key
        mock_client.secrets.kv.v2.read_secret_version.side_effect = InvalidPath()
        vault_client = _VaultClient(auth_type="approle", role_id="role", url="http://localhost:8180",
                                    secret_id="pass")
        secret = vault_client.get_secret(secret_path="missing")
        self.assertIsNone(secret)
        self.assertEqual("secret", vault_client.mount_point)
        mock_client.secrets.kv.v2.read_secret_version.assert_called_once_with(
            mount_point='secret', path='missing', version=None)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_get_non_existing_key_v1(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        # Response does not contain the requested key
        mock_client.secrets.kv.v1.read_secret.side_effect = InvalidPath()
        vault_client = _VaultClient(auth_type="approle", role_id="role", url="http://localhost:8180",
                                    secret_id="pass", kv_engine_version=1)
        secret = vault_client.get_secret(secret_path="missing")
        self.assertIsNone(secret)
        mock_client.secrets.kv.v1.read_secret.assert_called_once_with(
            mount_point='secret', path='missing')

    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_get_existing_key_v2(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

        mock_client.secrets.kv.v2.read_secret_version.return_value = {
            'request_id': '94011e25-f8dc-ec29-221b-1f9c1d9ad2ae',
            'lease_id': '',
            'renewable': False,
            'lease_duration': 0,
            'data': {
                'data': {'secret_key': 'secret_value'},
                'metadata': {'created_time': '2020-03-16T21:01:43.331126Z',
                             'deletion_time': '',
                             'destroyed': False,
                             'version': 1}},
            'wrap_info': None,
            'warnings': None,
            'auth': None
        }

        vault_client = _VaultClient(auth_type="approle", role_id="role", url="http://localhost:8180",
                                    secret_id="pass")
        secret = vault_client.get_secret(secret_path="missing")
        self.assertEqual({'secret_key': 'secret_value'}, secret)
        mock_client.secrets.kv.v2.read_secret_version.assert_called_once_with(
            mount_point='secret', path='missing', version=None)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_get_existing_key_v2_version(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

        mock_client.secrets.kv.v2.read_secret_version.return_value = {
            'request_id': '94011e25-f8dc-ec29-221b-1f9c1d9ad2ae',
            'lease_id': '',
            'renewable': False,
            'lease_duration': 0,
            'data': {
                'data': {'secret_key': 'secret_value'},
                'metadata': {'created_time': '2020-03-16T21:01:43.331126Z',
                             'deletion_time': '',
                             'destroyed': False,
                             'version': 1}},
            'wrap_info': None,
            'warnings': None,
            'auth': None
        }

        vault_client = _VaultClient(auth_type="approle", role_id="role", url="http://localhost:8180",
                                    secret_id="pass")
        secret = vault_client.get_secret(secret_path="missing", secret_version=1)
        self.assertEqual({'secret_key': 'secret_value'}, secret)
        mock_client.secrets.kv.v2.read_secret_version.assert_called_once_with(
            mount_point='secret', path='missing', version=1)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_get_existing_key_v1(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

        mock_client.secrets.kv.v1.read_secret.return_value = {
            'request_id': '182d0673-618c-9889-4cba-4e1f4cfe4b4b',
            'lease_id': '',
            'renewable': False,
            'lease_duration': 2764800,
            'data': {'value': 'world'},
            'wrap_info': None,
            'warnings': None,
            'auth': None}

        vault_client = _VaultClient(auth_type="approle", role_id="role", url="http://localhost:8180",
                                    secret_id="pass", kv_engine_version=1)
        secret = vault_client.get_secret(secret_path="missing")
        self.assertEqual({'value': 'world'}, secret)
        mock_client.secrets.kv.v1.read_secret.assert_called_once_with(
            mount_point='secret', path='missing')

    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_get_existing_key_v1_version(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        vault_client = _VaultClient(auth_type="token", token="s.7AU0I51yv1Q1lxOIg1F3ZRAS",
                                    url="http://localhost:8180", kv_engine_version=1)
        with self.assertRaisesRegex(VaultError, "Secret version"):
            vault_client.get_secret(secret_path="missing", secret_version=1)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_get_secret_metadata_v2(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_client.secrets.kv.v2.read_secret_metadata.return_value = {
            'request_id': '94011e25-f8dc-ec29-221b-1f9c1d9ad2ae',
            'lease_id': '',
            'renewable': False,
            'lease_duration': 0,
            'metadata': [
                {'created_time': '2020-03-16T21:01:43.331126Z',
                 'deletion_time': '',
                 'destroyed': False,
                 'version': 1},
                {'created_time': '2020-03-16T21:01:43.331126Z',
                 'deletion_time': '',
                 'destroyed': False,
                 'version': 2},
            ]
        }
        vault_client = _VaultClient(auth_type="token", token="s.7AU0I51yv1Q1lxOIg1F3ZRAS",
                                    url="http://localhost:8180")
        metadata = vault_client.get_secret_metadata(secret_path="missing")
        self.assertEqual(
            {
                'request_id': '94011e25-f8dc-ec29-221b-1f9c1d9ad2ae',
                'lease_id': '',
                'renewable': False,
                'lease_duration': 0,
                'metadata': [
                    {'created_time': '2020-03-16T21:01:43.331126Z',
                     'deletion_time': '',
                     'destroyed': False,
                     'version': 1},
                    {'created_time': '2020-03-16T21:01:43.331126Z',
                     'deletion_time': '',
                     'destroyed': False,
                     'version': 2},
                ]
            }, metadata)
        mock_client.secrets.kv.v2.read_secret_metadata.assert_called_once_with(
            mount_point='secret', path='missing')

    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_get_secret_metadata_v1(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

        vault_client = _VaultClient(auth_type="approle", role_id="role", url="http://localhost:8180",
                                    secret_id="pass", kv_engine_version=1)
        with self.assertRaisesRegex(VaultError, "Metadata might only be used with"
                                                " version 2 of the KV engine."):
            vault_client.get_secret_metadata(secret_path="missing")

    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_get_secret_including_metadata_v2(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

        mock_client.secrets.kv.v2.read_secret_version.return_value = {
            'request_id': '94011e25-f8dc-ec29-221b-1f9c1d9ad2ae',
            'lease_id': '',
            'renewable': False,
            'lease_duration': 0,
            'data': {
                'data': {'secret_key': 'secret_value'},
                'metadata': {'created_time': '2020-03-16T21:01:43.331126Z',
                             'deletion_time': '',
                             'destroyed': False,
                             'version': 1}},
            'wrap_info': None,
            'warnings': None,
            'auth': None
        }
        vault_client = _VaultClient(auth_type="approle", role_id="role", url="http://localhost:8180",
                                    secret_id="pass")
        metadata = vault_client.get_secret_including_metadata(secret_path="missing")
        self.assertEqual(
            {
                'request_id': '94011e25-f8dc-ec29-221b-1f9c1d9ad2ae',
                'lease_id': '',
                'renewable': False,
                'lease_duration': 0,
                'data': {
                    'data': {'secret_key': 'secret_value'},
                    'metadata': {'created_time': '2020-03-16T21:01:43.331126Z',
                                 'deletion_time': '',
                                 'destroyed': False,
                                 'version': 1}},
                'wrap_info': None,
                'warnings': None,
                'auth': None
            }, metadata)
        mock_client.secrets.kv.v2.read_secret_version.assert_called_once_with(
            mount_point='secret', path='missing', version=None)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_get_secret_including_metadata_v1(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

        vault_client = _VaultClient(auth_type="approle", role_id="role", url="http://localhost:8180",
                                    secret_id="pass", kv_engine_version=1)
        with self.assertRaisesRegex(VaultError, "Metadata might only be used with"
                                                " version 2 of the KV engine."):
            vault_client.get_secret_including_metadata(secret_path="missing")

    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_create_or_update_secret_v2(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

        vault_client = _VaultClient(auth_type="approle", role_id="role", url="http://localhost:8180",
                                    secret_id="pass")
        vault_client.create_or_update_secret(
            secret_path="path",
            secret={'key': 'value'}
        )
        mock_client.secrets.kv.v2.create_or_update_secret.assert_called_once_with(
            mount_point='secret', secret_path='path', secret={'key': 'value'}, cas=None)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_create_or_update_secret_v2_method(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

        vault_client = _VaultClient(auth_type="approle", role_id="role", url="http://localhost:8180",
                                    secret_id="pass")
        with self.assertRaisesRegex(VaultError, "The method parameter is only valid for version 1"):
            vault_client.create_or_update_secret(
                secret_path="path",
                secret={'key': 'value'},
                method="post"
            )

    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_create_or_update_secret_v2_cas(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

        vault_client = _VaultClient(auth_type="approle", role_id="role", url="http://localhost:8180",
                                    secret_id="pass")
        vault_client.create_or_update_secret(
            secret_path="path",
            secret={'key': 'value'},
            cas=10
        )
        mock_client.secrets.kv.v2.create_or_update_secret.assert_called_once_with(
            mount_point='secret', secret_path='path', secret={'key': 'value'}, cas=10)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_create_or_update_secret_v1(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

        vault_client = _VaultClient(auth_type="approle", role_id="role", url="http://localhost:8180",
                                    secret_id="pass", kv_engine_version=1)
        vault_client.create_or_update_secret(
            secret_path="path",
            secret={'key': 'value'}
        )
        mock_client.secrets.kv.v1.create_or_update_secret.assert_called_once_with(
            mount_point='secret', secret_path='path', secret={'key': 'value'}, method=None)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_create_or_update_secret_v1_cas(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

        vault_client = _VaultClient(auth_type="approle", role_id="role", url="http://localhost:8180",
                                    secret_id="pass", kv_engine_version=1)
        with self.assertRaisesRegex(VaultError, "The cas parameter is only valid for version 2"):
            vault_client.create_or_update_secret(
                secret_path="path",
                secret={'key': 'value'},
                cas=10
            )

    @mock.patch("airflow.providers.hashicorp.hooks.vault.hvac")
    def test_create_or_update_secret_v1_post(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

        vault_client = _VaultClient(auth_type="approle", role_id="role", url="http://localhost:8180",
                                    secret_id="pass", kv_engine_version=1)
        vault_client.create_or_update_secret(
            secret_path="path",
            secret={'key': 'value'},
            method="post"
        )
        mock_client.secrets.kv.v1.create_or_update_secret.assert_called_once_with(
            mount_point='secret', secret_path='path', secret={'key': 'value'}, method="post")
