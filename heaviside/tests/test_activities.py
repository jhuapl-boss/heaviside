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
from io import StringIO

from botocore.exceptions import ClientError

try:
    from unittest import mock
except ImportError:
    import mock

from .utils import MockPath, MockSession

from heaviside.activities import TaskMixin, ActivityMixin
from heaviside.activities import fanout, fanout_nonblocking, SFN
from heaviside.exceptions import ActivityError

# Suppress message about not handler for log messages
import logging
log = logging.getLogger("heaviside.activities")
log.addHandler(logging.NullHandler())
#log.addHandler(logging.StreamHandler())

class TimeoutError(ClientError):
    def __init__(self):
        op_name = "Test"
        err_rsp = {
            'Error': {
                'Code': 'TaskTimedOut'
            }
        }

        super(TimeoutError, self).__init__(err_rsp, op_name)

class BossError(Exception):
    pass

class TestFanout(unittest.TestCase):
    @mock.patch.object(SFN, 'create_name')
    @mock.patch('heaviside.activities.time.sleep')
    def test_args_generator(self, mSleep, mCreateName):
        mCreateName.return_value = 'ZZZ'
        iSession = MockSession()
        client = iSession.client('stepfunctions')
        client.list_state_machines.return_value = {
            'stateMachines':[{
                'stateMachineArn': 'XXX'
            }]
        }
        client.start_execution.return_value = {
            'executionArn': 'YYY'
        }
        client.describe_execution.return_value = {
            'status': 'SUCCEEDED',
            'output': 'null'
        }

        expected = [None]
        actual = fanout(iSession,
                        'XXX',
                        (i for i in range(0,1)))

        self.assertEqual(actual, expected)

        calls = [
            mock.call.list_state_machines(),
            mock.call.start_execution(stateMachineArn = 'XXX',
                                      name = 'ZZZ',
                                      input = '0'),
            mock.call.list_state_machines(),
            mock.call.describe_execution(executionArn = 'YYY')
        ]

        self.assertEqual(client.mock_calls, calls)

    @mock.patch.object(SFN, 'create_name')
    @mock.patch('heaviside.activities.time.sleep')
    def test_args_list(self, mSleep, mCreateName):
        mCreateName.return_value = 'ZZZ'
        iSession = MockSession()
        client = iSession.client('stepfunctions')
        client.list_state_machines.return_value = {
            'stateMachines':[{
                'stateMachineArn': 'XXX'
            }]
        }
        client.start_execution.return_value = {
            'executionArn': 'YYY'
        }
        client.describe_execution.return_value = {
            'status': 'SUCCEEDED',
            'output': 'null'
        }

        expected = [None]
        actual = fanout(iSession,
                        'XXX',
                        [i for i in range(0,1)])

        self.assertEqual(actual, expected)

        calls = [
            mock.call.list_state_machines(),
            mock.call.start_execution(stateMachineArn = 'XXX',
                                      name = 'ZZZ',
                                      input = '0'),
            mock.call.list_state_machines(),
            mock.call.describe_execution(executionArn = 'YYY')
        ]

        self.assertEqual(client.mock_calls, calls)

    @mock.patch.object(SFN, 'create_name')
    @mock.patch('heaviside.activities.time.sleep')
    def test_gt_concurrent(self, mSleep, mCreateName):
        mCreateName.return_value = 'ZZZ'
        iSession = MockSession()
        client = iSession.client('stepfunctions')
        client.list_state_machines.return_value = {
            'stateMachines':[{
                'stateMachineArn': 'XXX'
            }]
        }
        client.start_execution.return_value = {
            'executionArn': 'YYY'
        }
        client.describe_execution.return_value = {
            'status': 'SUCCEEDED',
            'output': 'null'
        }

        expected = [None, None]
        actual = fanout(iSession,
                        'XXX',
                        [i for i in range(0,2)],
                        max_concurrent=1)

        self.assertEqual(actual, expected)

        calls = [
            mock.call.list_state_machines(),
            mock.call.start_execution(stateMachineArn = 'XXX',
                                      name = 'ZZZ',
                                      input = '0'),
            mock.call.list_state_machines(),
            mock.call.describe_execution(executionArn = 'YYY'),
            mock.call.start_execution(stateMachineArn = 'XXX',
                                      name = 'ZZZ',
                                      input = '1'),
            mock.call.list_state_machines(),
            mock.call.describe_execution(executionArn = 'YYY'),
        ]

        self.assertEqual(client.mock_calls, calls)

    @mock.patch.object(SFN, 'create_name')
    @mock.patch('heaviside.activities.time.sleep')
    def test_sfn_error(self, mSleep, mCreateName):
        mCreateName.return_value = 'ZZZ'
        iSession = MockSession()
        client = iSession.client('stepfunctions')
        client.list_state_machines.return_value = {
            'stateMachines':[{
                'stateMachineArn': 'XXX'
            }]
        }
        client.start_execution.side_effect = [
            { 'executionArn': 'YYY' },
            { 'executionArn': 'YYYY' }
        ]
        client.describe_execution.side_effect = [
            { 'status': 'FAILED' },
            { 'status': 'RUNNING' }
        ]
        client.get_execution_history.return_value = {
            'events':[{
                'executionFailedEventDetails':{
                    'error': 'error',
                    'cause': 'cause'
                }
            }]
        }

        try:
            fanout(iSession,
                   'XXX',
                   [i for i in range(0,2)])
            self.assertFalse(True, "fanout should result in an ActivityError")
        except ActivityError as e:
            self.assertEqual(e.error, 'error')
            self.assertEqual(e.cause, 'cause')

        calls = [
            mock.call.list_state_machines(),
            mock.call.start_execution(stateMachineArn = 'XXX',
                                      name = 'ZZZ',
                                      input = '0'),
            mock.call.start_execution(stateMachineArn = 'XXX',
                                      name = 'ZZZ',
                                      input = '1'),
            mock.call.list_state_machines(),
            mock.call.describe_execution(executionArn = 'YYY'),
            mock.call.get_execution_history(executionArn = 'YYY',
                                            reverseOrder = True),
            mock.call.describe_execution(executionArn = 'YYYY'),
            mock.call.stop_execution(executionArn = 'YYYY',
                                     error = "Heaviside.Fanout",
                                     cause = "Sub-process error detected")
        ]

        self.assertEqual(client.mock_calls, calls)

class TestFanoutNonBlocking(unittest.TestCase):
    @mock.patch.object(SFN, 'create_name')
    @mock.patch('heaviside.activities.time.sleep')
    def test_gt_concurrent(self, mSleep, mCreateName):
        mCreateName.return_value = 'ZZZ'
        iSession = MockSession()
        client = iSession.client('stepfunctions')
        client.list_state_machines.return_value = {
            'stateMachines':[{
                'stateMachineArn': 'XXX'
            }]
        }
        client.start_execution.return_value = {
            'executionArn': 'YYY'
        }
        client.describe_execution.return_value = {
            'status': 'SUCCEEDED',
            'output': 'null'
        }

        args = {
            'sub_sfn': 'XXX',
            'common_sub_args': {},
            'sub_args': [i for i in range(0,2)],
            'max_concurrent': 1,
            'rampup_delay': 10,
            'rampup_backoff': 0.5,
            'status_delay': 0,
            'finished': False,
            'running': [],
            'results': [],
        }

        args1 = fanout_nonblocking(args, iSession)
        self.assertFalse(args1['finished'])
        self.assertEqual(args1['running'], ['YYY'])
        self.assertEqual(args1['results'], [])
        self.assertEqual(args1['rampup_delay'], 5)

        args2 = fanout_nonblocking(args, iSession)
        self.assertFalse(args2['finished'])
        self.assertEqual(args2['running'], ['YYY'])
        self.assertEqual(args2['results'], [None])
        self.assertEqual(args2['rampup_delay'], 2)

        args3 = fanout_nonblocking(args, iSession)
        self.assertTrue(args3['finished'])
        self.assertEqual(args3['running'], [])
        self.assertEqual(args3['results'], [None, None])
        self.assertEqual(args3['rampup_delay'], 2) # no processes launched

        calls = [
            mock.call.list_state_machines(),
            mock.call.start_execution(stateMachineArn = 'XXX',
                                      name = 'ZZZ',
                                      input = '0'),
            mock.call.list_state_machines(),
            mock.call.describe_execution(executionArn = 'YYY'),
            mock.call.start_execution(stateMachineArn = 'XXX',
                                      name = 'ZZZ',
                                      input = '1'),
            mock.call.list_state_machines(),
            mock.call.describe_execution(executionArn = 'YYY'),
        ]

        self.assertEqual(client.mock_calls, calls)


class TestTaskMixin(unittest.TestCase):
    @mock.patch('heaviside.activities.create_session', autospec=True)
    def test_success(self, mCreateSession):
        iSession = MockSession()
        client = iSession.client('stepfunctions')
        mCreateSession.return_value = (iSession, '123456')

        task = TaskMixin()
        task.token = 'token'
        
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

        task = TaskMixin()
        
        with self.assertRaises(Exception):
            task.success(None)

        self.assertEqual(task.token, None)

    @mock.patch('heaviside.activities.create_session', autospec=True)
    def test_success_timeout(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')

        client = iSession.client('stepfunctions')
        client.send_task_success.side_effect = TimeoutError()

        task = TaskMixin()
        task.token = 'token'

        task.success(None)

        self.assertEqual(task.token, None)

    @mock.patch('heaviside.activities.create_session', autospec=True)
    def test_failure(self, mCreateSession):
        iSession = MockSession()
        client = iSession.client('stepfunctions')
        mCreateSession.return_value = (iSession, '123456')

        task = TaskMixin()
        task.token = 'token'
        
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

        task = TaskMixin()
        
        with self.assertRaises(Exception):
            task.failure(None, None)

        self.assertEqual(task.token, None)

    @mock.patch('heaviside.activities.create_session', autospec=True)
    def test_failure_timeout(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')

        client = iSession.client('stepfunctions')
        client.send_task_failure.side_effect = TimeoutError()

        task = TaskMixin()
        task.token = 'token'

        task.failure(None, None)

        self.assertEqual(task.token, None)

    @mock.patch('heaviside.activities.create_session', autospec=True)
    def test_heartbeat(self, mCreateSession):
        iSession = MockSession()
        client = iSession.client('stepfunctions')
        mCreateSession.return_value = (iSession, '123456')

        task = TaskMixin()
        task.token = 'token'
        task.heartbeat()

        call = mock.call.send_task_heartbeat(taskToken = 'token')
        self.assertEqual(client.mock_calls, [call])

    @mock.patch('heaviside.activities.create_session', autospec=True)
    def test_heartbeat_no_token(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')

        task = TaskMixin()
        
        with self.assertRaises(Exception):
            task.heartbeat()

    @mock.patch('heaviside.activities.create_session', autospec=True)
    def test_run_function(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')
        client = iSession.client('stepfunctions')

        task = TaskMixin()
        task.handle_task('token', None)

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
            return

        # Just make sure the target is actually a generator
        self.assertEqual(type(target(None)), types.GeneratorType)

        task = TaskMixin(process = target)
        task.handle_task('token', None)

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

        task = TaskMixin(process = target)
        task.handle_task('token', None)

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

        task = TaskMixin(process = target)
        task.handle_task('token', None)

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

        task = TaskMixin(process = target)
        task.handle_task('token', None)

        self.assertEqual(task.token, None)
        self.assertEqual(client.mock_calls, [])

class TestActivityMixin(unittest.TestCase):
    """
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

        calls = [
            mock.call.list_activities()
        ]
        self.assertEqual(client.mock_calls, calls)
    """

    # DP ???: How to test the import

    @mock.patch('heaviside.activities.create_session', autospec=True)
    def test_create_activity(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')
        client = iSession.client('stepfunctions')
        client.create_activity.return_value = {
            'activityArn': 'XXX'
        }

        activity = ActivityMixin()
        activity.name = 'name'

        self.assertEqual(activity.arn, None)

        activity.create_activity()

        self.assertEqual(activity.arn, 'XXX')

        calls = [
            mock.call.create_activity(name = 'name')
        ]
        self.assertEqual(client.mock_calls, calls)

    @mock.patch('heaviside.activities.create_session', autospec=True)
    def test_create_activity_exists(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')
        client = iSession.client('stepfunctions')

        activity = ActivityMixin()
        activity.arn = 'XXX'

        activity.create_activity()

        self.assertEqual(activity.arn, 'XXX')

        calls = [
        ]
        self.assertEqual(client.mock_calls, calls)

    @mock.patch('heaviside.activities.create_session', autospec=True)
    def test_create_activity_exception(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')
        client = iSession.client('stepfunctions')

        activity = ActivityMixin()
        activity.arn = 'XXX'

        with self.assertRaises(Exception):
            activity.create_activity(exception=True)

        self.assertEqual(activity.arn, 'XXX')

        calls = [
        ]
        self.assertEqual(client.mock_calls, calls)

    @mock.patch('heaviside.activities.create_session', autospec=True)
    def test_delete_activity(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')
        client = iSession.client('stepfunctions')

        activity = ActivityMixin()
        activity.arn = 'XXX'

        activity.delete_activity()

        self.assertEqual(activity.arn, None)

        calls = [
            mock.call.delete_activity(activityArn = 'XXX')
        ]
        self.assertEqual(client.mock_calls, calls)

    @mock.patch('heaviside.activities.create_session', autospec=True)
    def test_delete_doesnt_exist(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')
        client = iSession.client('stepfunctions')

        activity = ActivityMixin()

        self.assertEqual(activity.arn, None)

        activity.delete_activity()

        self.assertEqual(activity.arn, None)

        calls = [
        ]
        self.assertEqual(client.mock_calls, calls)

    @mock.patch('heaviside.activities.create_session', autospec=True)
    def test_delete_activity_exception(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')
        client = iSession.client('stepfunctions')

        activity = ActivityMixin()

        self.assertEqual(activity.arn, None)

        with self.assertRaises(Exception):
            activity.delete_activity(exception=True)

        self.assertEqual(activity.arn, None)

        calls = [
        ]
        self.assertEqual(client.mock_calls, calls)

    @mock.patch('heaviside.activities.create_session', autospec=True)
    def test_poll_task_exception(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')
        client = iSession.client('stepfunctions')

        activity = ActivityMixin()

        self.assertEqual(activity.arn, None)

        with self.assertRaises(Exception):
            activity.poll_task(worker = 'worker')

        calls = [
        ]
        self.assertEqual(client.mock_calls, calls)

    @mock.patch('heaviside.activities.create_session', autospec=True)
    def test_poll_task(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')
        client = iSession.client('stepfunctions')
        client.get_activity_task.return_value = {
            'taskToken': 'YYY',
            'input': '{}'
        }

        activity = ActivityMixin()
        activity.arn = 'XXX'

        token,  input_ = activity.poll_task('worker')

        self.assertEqual(token, 'YYY')
        self.assertEqual(input_, {})

        calls = [
            mock.call.get_activity_task(activityArn = 'XXX',
                                        workerName = 'worker')
        ]
        self.assertEqual(client.mock_calls, calls)

    @mock.patch('heaviside.activities.create_session', autospec=True)
    def test_poll_task_no_work(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')
        client = iSession.client('stepfunctions')
        client.get_activity_task.return_value = {
            'taskToken': ''
        }

        activity = ActivityMixin()
        activity.arn = 'XXX'

        token,  input_ = activity.poll_task('worker')

        self.assertEqual(token, None)
        self.assertEqual(input_, None)

        calls = [
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

        activity = ActivityMixin(handle_task = target)

        def stop_loop(*args, **kwargs):
            activity.polling = False
            return mock.DEFAULT
        target.side_effect = stop_loop

        activity.run('name')

        calls = [
            mock.call.list_activities(),
            mock.call.get_activity_task(activityArn = 'XXX',
                                        workerName = 'name-XXX')
        ]
        self.assertEqual(client.mock_calls, calls)

        calls = [
            mock.call('YYY', {}),
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

        activity = ActivityMixin()

        def stop_loop(*args, **kwargs):
            activity.polling = False
            raise BossError(None)
        client.get_activity_task.side_effect = stop_loop

        activity.run('name')

        calls = [
            mock.call.list_activities(),
            mock.call.get_activity_task(activityArn = 'XXX',
                                        workerName = 'name-XXX')
        ]
        self.assertEqual(client.mock_calls, calls)

