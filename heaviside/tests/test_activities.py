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

from heaviside.activities import TaskProcess, ActivityProcess
from heaviside.exceptions import ActivityError

class TimeoutError(ClientError):
    def __init__(self):
        op_name = "Test"
        err_rsp = {
            'Error': {
                'Code': 'TaskTimedOut'
            }
        }

        super().__init__(err_rsp, op_name)

class BossError(Exception):
    pass

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

    @mock.patch('heaviside.activities.create_session', autospec=True)
    def test_run_activity_error(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')
        client = iSession.client('stepfunctions')

        target = mock.MagicMock()
        target.side_effect = ActivityError('error', 'cause')

        task = TaskProcess('worker', 'token', 'null', target)

        self.assertEqual(task.token, 'token')

        task.run()

        self.assertEqual(task.token, None)
        call = mock.call.send_task_failure(taskToken = 'token',
                                           error = 'error',
                                           cause = 'cause')
        self.assertEqual(client.mock_calls, [call])

    @mock.patch('heaviside.activities.create_session', autospec=True)
    def test_run_exception(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')
        client = iSession.client('stepfunctions')

        target = mock.MagicMock()
        target.side_effect = BossError('cause')

        task = TaskProcess('worker', 'token', 'null', target)

        self.assertEqual(task.token, 'token')

        task.run()

        self.assertEqual(task.token, None)
        call = mock.call.send_task_failure(taskToken = 'token',
                                           error = 'BossError',
                                           cause = 'cause')
        self.assertEqual(client.mock_calls, [call])

    @mock.patch('heaviside.activities.create_session', autospec=True)
    def test_run_timeout(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')
        client = iSession.client('stepfunctions')

        target = mock.MagicMock()
        target.side_effect = TimeoutError()

        task = TaskProcess('worker', 'token', 'null', target)

        self.assertEqual(task.token, 'token')

        task.run()

        self.assertEqual(task.token, None)
        self.assertEqual(client.mock_calls, [])

class TestActivityProcess(unittest.TestCase):
    @mock.patch('heaviside.activities.create_session', autospec=True)
    def test_constructor(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')
        client = iSession.client('stepfunctions')
        client.list_activities.return_value = {
            'activities':[{
                'name': 'name',
                'activityArn': 'XXX'
            }]
        }

        activity = ActivityProcess('name', None)

        self.assertEqual(activity.arn, 'XXX')

    # DP ???: How to test the import

    @mock.patch('heaviside.activities.create_session', autospec=True)
    def test_create(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')
        client = iSession.client('stepfunctions')
        client.list_activities.return_value = {
            'activities':[{
                'name': 'name',
                'activityArn': None
            }]
        }
        client.create_activity.return_value = {
            'activityArn': 'XXX'
        }

        activity = ActivityProcess('name', None)

        self.assertEqual(activity.arn, None)

        activity.create()

        self.assertEqual(activity.arn, 'XXX')

        calls = [
            mock.call.list_activities(),
            mock.call.create_activity(name = 'name')
        ]
        self.assertEqual(client.mock_calls, calls)

    @mock.patch('heaviside.activities.create_session', autospec=True)
    def test_create_exists(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')
        client = iSession.client('stepfunctions')
        client.list_activities.return_value = {
            'activities':[{
                'name': 'name',
                'activityArn': 'XXX'
            }]
        }

        activity = ActivityProcess('name', None)

        self.assertEqual(activity.arn, 'XXX')

        activity.create()

        self.assertEqual(activity.arn, 'XXX')

        calls = [
            mock.call.list_activities(),
        ]
        self.assertEqual(client.mock_calls, calls)

    @mock.patch('heaviside.activities.create_session', autospec=True)
    def test_create_exception(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')
        client = iSession.client('stepfunctions')
        client.list_activities.return_value = {
            'activities':[{
                'name': 'name',
                'activityArn': 'XXX'
            }]
        }

        activity = ActivityProcess('name', None)

        self.assertEqual(activity.arn, 'XXX')

        with self.assertRaises(Exception):
            activity.create(True)

        self.assertEqual(activity.arn, 'XXX')

        calls = [
            mock.call.list_activities(),
        ]
        self.assertEqual(client.mock_calls, calls)

    @mock.patch('heaviside.activities.create_session', autospec=True)
    def test_delete(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')
        client = iSession.client('stepfunctions')
        client.list_activities.return_value = {
            'activities':[{
                'name': 'name',
                'activityArn': 'XXX'
            }]
        }

        activity = ActivityProcess('name', None)

        self.assertEqual(activity.arn, 'XXX')

        activity.delete()

        self.assertEqual(activity.arn, None)

        calls = [
            mock.call.list_activities(),
            mock.call.delete_activity(activityArn = 'XXX')
        ]
        self.assertEqual(client.mock_calls, calls)

    @mock.patch('heaviside.activities.create_session', autospec=True)
    def test_delete_doesnt_exist(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')
        client = iSession.client('stepfunctions')
        client.list_activities.return_value = {
            'activities':[{
                'name': 'name',
                'activityArn': None
            }]
        }

        activity = ActivityProcess('name', None)

        self.assertEqual(activity.arn, None)

        activity.delete()

        self.assertEqual(activity.arn, None)

        calls = [
            mock.call.list_activities(),
        ]
        self.assertEqual(client.mock_calls, calls)

    @mock.patch('heaviside.activities.create_session', autospec=True)
    def test_delete_exception(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')
        client = iSession.client('stepfunctions')
        client.list_activities.return_value = {
            'activities':[{
                'name': 'name',
                'activityArn': None
            }]
        }

        activity = ActivityProcess('name', None)

        self.assertEqual(activity.arn, None)

        with self.assertRaises(Exception):
            activity.delete(True)

        self.assertEqual(activity.arn, None)

        calls = [
            mock.call.list_activities(),
        ]
        self.assertEqual(client.mock_calls, calls)

    @mock.patch('heaviside.activities.create_session', autospec=True)
    def test_task_exception(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')
        client = iSession.client('stepfunctions')
        client.list_activities.return_value = {
            'activities':[{
                'name': 'name',
                'activityArn': None
            }]
        }

        activity = ActivityProcess('name', None)

        with self.assertRaises(Exception):
            activity.task('worker')

        calls = [
            mock.call.list_activities(),
        ]
        self.assertEqual(client.mock_calls, calls)

    @mock.patch('heaviside.activities.create_session', autospec=True)
    def test_task(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')
        client = iSession.client('stepfunctions')
        client.list_activities.return_value = {
            'activities':[{
                'name': 'name',
                'activityArn': 'XXX'
            }]
        }
        client.get_activity_task.return_value = {
            'taskToken': 'YYY',
            'input': '{}'
        }

        activity = ActivityProcess('name', None)
        token,  input_ = activity.task('worker')

        self.assertEqual(token, 'YYY')
        self.assertEqual(input_, {})

        calls = [
            mock.call.list_activities(),
            mock.call.get_activity_task(activityArn = 'XXX',
                                        workerName = 'worker')
        ]
        self.assertEqual(client.mock_calls, calls)

    @mock.patch('heaviside.activities.create_session', autospec=True)
    def test_task_no_work(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')
        client = iSession.client('stepfunctions')
        client.list_activities.return_value = {
            'activities':[{
                'name': 'name',
                'activityArn': 'XXX'
            }]
        }
        client.get_activity_task.return_value = {
            'taskToken': ''
        }

        activity = ActivityProcess('name', None)
        token,  input_ = activity.task('worker')

        self.assertEqual(token, None)
        self.assertEqual(input_, None)

        calls = [
            mock.call.list_activities(),
            mock.call.get_activity_task(activityArn = 'XXX',
                                        workerName = 'worker')
        ]
        self.assertEqual(client.mock_calls, calls)

    @mock.patch('heaviside.activities.random.sample', autospec=True)
    @mock.patch('heaviside.activities.create_session', autospec=True)
    def test_run(self, mCreateSession, mSample):
        mSample.return_value = 'XXX'
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')
        client = iSession.client('stepfunctions')
        client.list_activities.return_value = {
            'activities':[{
                'name': 'name',
                'activityArn': 'XXX'
            }]
        }
        client.get_activity_task.return_value = {
            'taskToken': 'YYY',
            'input': '{}'
        }

        target = mock.MagicMock()

        activity = ActivityProcess('name', target)

        def stop_loop(*args, **kwargs):
            activity.running = False
            return mock.DEFAULT
        target.side_effect = stop_loop

        activity.run()

        calls = [
            mock.call.list_activities(),
            mock.call.get_activity_task(activityArn = 'XXX',
                                        workerName = 'name-XXX')
        ]
        self.assertEqual(client.mock_calls, calls)

        calls = [
            mock.call('name-XXX', 'YYY', {}),
            mock.call().start()
        ]
        self.assertEqual(target.mock_calls, calls)

    @mock.patch('heaviside.activities.random.sample', autospec=True)
    @mock.patch('heaviside.activities.create_session', autospec=True)
    def test_run_exception(self, mCreateSession, mSample):
        mSample.return_value = 'XXX'
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')
        client = iSession.client('stepfunctions')
        client.list_activities.return_value = {
            'activities':[{
                'name': 'name',
                'activityArn': 'XXX'
            }]
        }

        activity = ActivityProcess('name', None)

        def stop_loop(*args, **kwargs):
            activity.running = False
            raise BossError(None)
        client.get_activity_task.side_effect = stop_loop

        activity.run()

        calls = [
            mock.call.list_activities(),
            mock.call.get_activity_task(activityArn = 'XXX',
                                        workerName = 'name-XXX')
        ]
        self.assertEqual(client.mock_calls, calls)

