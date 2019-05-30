# Copyright 2016 The Johns Hopkins University Applied Physics Laboratory
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import unicode_literals

import unittest
import sys
from io import StringIO

try:
    from unittest import mock
except ImportError:
    import mock

from .utils import MockPath, MockSession
from heaviside import utils

class TestRead(unittest.TestCase):
    def test_path(self):
        name = "/path/to/file"
        path = MockPath(name)

        with utils.read(path) as fh:
            pass

        # close() is on the fh returned by open()
        calls = [mock.call(), mock.call().close()]
        self.assertEqual(path.open.mock_calls, calls)

    def test_path_close(self):
        name = "/path/to/file"
        path = MockPath(name)

        with self.assertRaises(Exception):
            with utils.read(path) as fh:
                raise Exception()

        # close() is on the fh returned by open()
        calls = [mock.call(), mock.call().close()]
        self.assertEqual(path.open.mock_calls, calls)

    def test_string(self):
        data = "foo bar boo"

        with utils.read(data) as fh:
            self.assertEqual(data, fh.read())
            self.assertEqual('<string>', fh.name)

    def test_fileobj(self):
        data = "foo bar boo"
        obj = StringIO(data)

        with utils.read(obj) as fh:
            self.assertEqual(obj, fh)
            self.assertEqual(data, fh.read())

    def test_unsuported(self):
        with self.assertRaises(ValueError):
            with utils.read(None) as fh:
                pass

class TestWrite(unittest.TestCase):
    def test_stdout(self):
        with utils.write('-') as fh:
            self.assertEqual(sys.stdout, fh)

    @mock.patch.object(utils.Path, 'open')
    def test_string(self, mOpen):
        path = '/path/to/file'
        data = "foo bar boo"

        with utils.write(path) as fh:
            fh.write(data)

        expected = [mock.call('w'),
                    mock.call().write(data),
                    mock.call().close()]
        self.assertEqual(mOpen.mock_calls, expected)

    @mock.patch.object(utils.Path, 'open')
    def test_path(self, mOpen):
        path = utils.Path('/path/to/file')
        data = "foo bar boo"

        with utils.write(path) as fh:
            fh.write(data)

        expected = [mock.call('w'),
                    mock.call().write(data),
                    mock.call().close()]
        self.assertEqual(mOpen.mock_calls, expected)

    def test_fileobj(self):
        data = "foo bar boo"
        obj = StringIO()

        with utils.write(obj) as fh:
            self.assertEqual(obj, fh)
            fh.write(data)

        obj.seek(0)
        self.assertEqual(data, obj.read())

    def test_unsuported(self):
        with self.assertRaises(ValueError):
            with utils.write(None) as fh:
                pass

class TestCreateSession(unittest.TestCase):
    @mock.patch('heaviside.utils.Session', autospec=True)
    def test_noargs(self, mSession):
        iSession = mSession.return_value = MockSession()
        account = '123456'
        region = 'east'

        session, account_id = utils.create_session(account_id=account, region=region)

        self.assertEqual(session, iSession)
        self.assertEqual(account_id, account)
        self.assertEqual(iSession.region, region)

        calls = [mock.call()]
        self.assertEqual(mSession.mock_calls, calls)

    @mock.patch('heaviside.utils.Session', autospec=True)
    def test_session(self, mSession):
        iSession = MockSession()
        account = '123456'

        session, account_id = utils.create_session(account_id=account, session=iSession)

        self.assertEqual(session, iSession)
        self.assertEqual(account_id, account)
        self.assertEqual(mSession.mock_calls, [])

    @mock.patch('heaviside.utils.Session', autospec=True)
    def test_credentials(self, mSession):
        iSession = mSession.return_value = MockSession()
        access = 'XXX'
        secret = 'YYY'
        account = '123456'
        region = 'east'
        credentials = {
            'aws_access_key': access,
            'aws_secret_key': secret,
            'aws_account_id': account,
            'aws_region': region
        }

        session, account_id = utils.create_session(credentials = credentials)

        self.assertEqual(session, iSession)
        self.assertEqual(account_id, account)
        self.assertEqual(iSession.region, region)

        call = mock.call(aws_access_key_id = access,
                         aws_secret_access_key = secret)
        calls = [call]
        self.assertEqual(mSession.mock_calls, calls)

    @mock.patch('heaviside.utils.Session', autospec=True)
    def test_keys(self, mSession):
        iSession = mSession.return_value = MockSession()
        access = 'XXX'
        secret = 'YYY'
        account = '123456'
        region = 'east'
        credentials = {
            'aws_access_key': access,
            'aws_secret_key': secret,
            'aws_account_id': account,
            'aws_region': region
        }

        # Note: Same as test_credentials, except passing the arguments are key word args
        session, account_id = utils.create_session(**credentials)

        self.assertEqual(session, iSession)
        self.assertEqual(account_id, account)
        self.assertEqual(iSession.region, region)

        call = mock.call(aws_access_key_id = access,
                         aws_secret_access_key = secret)
        calls = [call]
        self.assertEqual(mSession.mock_calls, calls)

    def test_lookup_account(self):
        # only check the logic for looking up the account id
        # test_session will verify the rest of the logic is still working
        iSession = MockSession()
        client = iSession.client('iam')
        client.list_users.return_value = {
            'Users': [{
                'Arn': '1:2:3:4:5:6'
            }]
        }

        session, account_id = utils.create_session(session=iSession)

        self.assertEqual(account_id, '5')

        calls = [mock.call.list_users(MaxItems=1)]
        self.assertEqual(client.mock_calls, calls)

    @mock.patch('heaviside.utils.urlopen', autospec=True)
    @mock.patch('heaviside.utils.Session', autospec=True)
    def test_lookup_region(self, mSession, mUrlOpen):
        # only check the logic for looking up the region
        # test_noargs will verify the rest of the logic is still working
        mSession.return_value = MockSession()
        region = 'east'
        az = region + 'a'
        mUrlOpen.return_value.read.return_value = az.encode()

        session, account_id = utils.create_session(account_id='12345')

        self.assertEqual(session.region, region)

