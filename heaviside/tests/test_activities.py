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

import types
import unittest
from unittest import mock
from io import StringIO

from botocore.exceptions import ClientError

from .utils import MockPath, MockSession
from heaviside.activities import TaskProcess

class TimeoutError(ClientError):
    def __init__(self):
        op_name = "Test"
        err_rsp = {
            'Error': {
                'Code': 'TaskTimedOut'
            }
        }

        super().__init__(err_rsp, op_name)

class TestTaskProcess(unittest.TestCase):
    @mock.patch('heaviside.activities.create_session', autospec=True)
    def test_success(self, mCreateSession):
        iSession = MockSession()
        client = iSession.client('stepfunctions')
        mCreateSession.return_value = (iSession, '123456')

        task = TaskProcess('worker', 'token', 'null')
        
        self.assertEqual(task.token, 'token')

        task.success(None)

        self.assertEqual(task.token, None)
        call = mock.call.send_task_success(taskToken = 'token',
                                           output = 'null')
        self.assertEqual(client.mock_calls, [call])

    @mock.patch('heaviside.activities.create_session', autospec=True)
    def test_success_no_token(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')

        task = TaskProcess('worker', None, 'null')
        
        with self.assertRaises(Exception):
            task.success(None)

        self.assertEqual(task.token, None)

    @mock.patch('heaviside.activities.create_session', autospec=True)
    def test_success_timeout(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')

        client = iSession.client('stepfunctions')
        client.send_task_success.side_effect = TimeoutError()

        task = TaskProcess('worker', 'token', 'null')

        task.success(None)

        self.assertEqual(task.token, None)

    @mock.patch('heaviside.activities.create_session', autospec=True)
    def test_failure(self, mCreateSession):
        iSession = MockSession()
        client = iSession.client('stepfunctions')
        mCreateSession.return_value = (iSession, '123456')

        task = TaskProcess('worker', 'token', 'null')
        
        self.assertEqual(task.token, 'token')

        task.failure(None, None)

        self.assertEqual(task.token, None)
        call = mock.call.send_task_failure(taskToken = 'token',
                                           error = None,
                                           cause = None)
        self.assertEqual(client.mock_calls, [call])

    @mock.patch('heaviside.activities.create_session', autospec=True)
    def test_failure_no_token(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')

        task = TaskProcess('worker', None, 'null')
        
        with self.assertRaises(Exception):
            task.failure(None, None)

        self.assertEqual(task.token, None)

    @mock.patch('heaviside.activities.create_session', autospec=True)
    def test_failure_timeout(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')

        client = iSession.client('stepfunctions')
        client.send_task_failure.side_effect = TimeoutError()

        task = TaskProcess('worker', 'token', 'null')

        task.failure(None, None)

        self.assertEqual(task.token, None)

    @mock.patch('heaviside.activities.create_session', autospec=True)
    def test_heartbeat(self, mCreateSession):
        iSession = MockSession()
        client = iSession.client('stepfunctions')
        mCreateSession.return_value = (iSession, '123456')

        task = TaskProcess('worker', 'token', 'null')
        task.heartbeat()

        call = mock.call.send_task_heartbeat(taskToken = 'token')
        self.assertEqual(client.mock_calls, [call])

    @mock.patch('heaviside.activities.create_session', autospec=True)
    def test_heartbeat_no_token(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')

        task = TaskProcess('worker', None, 'null')
        
        with self.assertRaises(Exception):
            task.heartbeat()

    @mock.patch('heaviside.activities.create_session', autospec=True)
    def test_run_function(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')
        client = iSession.client('stepfunctions')

        target = mock.MagicMock()
        target.return_value = None

        task = TaskProcess('worker', 'token', 'null', target)

        self.assertEqual(task.token, 'token')

        task.run()

        self.assertEqual(task.token, None)
        call = mock.call.send_task_success(taskToken = 'token',
                                           output = 'null')
        self.assertEqual(client.mock_calls, [call])

    @mock.patch('heaviside.activities.create_session', autospec=True)
    def test_run_generator(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')
        client = iSession.client('stepfunctions')

        def target(input_):
            yield
            yield
            return None

        # Just make sure the target is actually a generator
        self.assertEqual(type(target(None)), types.GeneratorType)

        task = TaskProcess('worker', 'token', 'null', target)

        self.assertEqual(task.token, 'token')

        task.run()

        self.assertEqual(task.token, None)
        call = mock.call.send_task_success(taskToken = 'token',
                                           output = 'null')
        call_ = mock.call.send_task_heartbeat(taskToken = 'token')
        calls = [call_, call_, call]
        self.assertEqual(client.mock_calls, calls)

    def test_run_activity_error(self):
        pass

    def test_run_exception(self):
        pass

    def test_run_timeout(self):
        pass

