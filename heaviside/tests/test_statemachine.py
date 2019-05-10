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

import unittest

try:
    from unittest import mock
except ImportError:
    import mock

from .utils import MockSession

from heaviside import StateMachine

class TestStateMachine(unittest.TestCase):
    @mock.patch('heaviside.create_session', autospec=True)
    def test_constructor(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')
        client = iSession.client('stepfunctions')
        client.list_state_machines.return_value = {
            'stateMachines':[{
                'name': 'name',
                'stateMachineArn': 'XXX'
            }]
        }

        machine = StateMachine('name')

        self.assertEqual(machine.arn, 'XXX')

        calls = [
            mock.call.list_state_machines()
        ]
        self.assertEqual(client.mock_calls, calls)

    @mock.patch('heaviside.compile', autospec=True)
    @mock.patch('heaviside.create_session', autospec=True)
    def test_build(self, mCreateSession, mCompile):
        iSession = MockSession()
        iSession.region_name = 'region'
        mCreateSession.return_value = (iSession, '123456')
        client = iSession.client('stepfunctions')
        client.list_state_machines.return_value = {
            'stateMachines':[{
                'name': 'name',
                'stateMachineArn': 'XXX'
            }]
        }

        mCompile.return_value = {}

        machine = StateMachine('name')

        sfn = "Pass()"
        actual = machine.build(sfn)
        expected = {}

        self.assertEqual(actual, expected)
        
        calls = [
            mock.call(sfn, region=machine.region, account_id=machine.account_id, visitors=[])
        ]
        self.assertEqual(mCompile.mock_calls, calls)

    @mock.patch('heaviside.create_session', autospec=True)
    def test_resolve_role(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')

        client = iSession.client('stepfunctions')
        client.list_state_machines.return_value = {
            'stateMachines':[{
                'name': 'name',
                'stateMachineArn': 'XXX'
            }]
        }

        iam = iSession.client('iam')
        iam.get_role.return_value = {
            'Role': {
                'Arn': 'YYY'
            }
        }

        machine = StateMachine('name')
        arn = machine._resolve_role('role')

        self.assertEqual(arn, 'YYY')

        calls = [
            mock.call.get_role(RoleName = 'role')
        ]
        self.assertEqual(iam.mock_calls, calls)

    @mock.patch('heaviside.create_session', autospec=True)
    def test_create(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')

        client = iSession.client('stepfunctions')
        client.list_state_machines.return_value = {
            'stateMachines':[]
        }

        client.create_state_machine.return_value = {
            'stateMachineArn': 'XXX'
        }

        _resolve_role = mock.MagicMock()
        _resolve_role.return_value = 'arn'

        build = mock.MagicMock()
        build.return_value = {}

        machine = StateMachine('name')
        machine._resolve_role = _resolve_role
        machine.build = build

        machine.create('source', 'role')

        self.assertEqual(machine.arn, 'XXX')

        calls = [mock.call('role')]
        self.assertEqual(_resolve_role.mock_calls, calls)

        calls = [mock.call('source')]
        self.assertEqual(build.mock_calls, calls)

        calls = [
            mock.call.list_state_machines(),
            mock.call.create_state_machine(name = 'name',
                                           definition = {},
                                           roleArn = 'arn')
        ]
        self.assertEqual(client.mock_calls, calls)

    @mock.patch('heaviside.create_session', autospec=True)
    def test_create_exists(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')

        client = iSession.client('stepfunctions')
        client.list_state_machines.return_value = {
            'stateMachines':[{
                'name': 'name',
                'stateMachineArn': 'XXX'
            }]
        }

        machine = StateMachine('name')

        with self.assertRaises(Exception):
            machine.create('source', 'role')

        self.assertEqual(machine.arn, 'XXX')

        calls = [
            mock.call.list_state_machines(),
        ]
        self.assertEqual(client.mock_calls, calls)

    @mock.patch('heaviside.create_session', autospec=True)
    def test_delete(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')

        client = iSession.client('stepfunctions')
        client.list_state_machines.return_value = {
            'stateMachines':[{
                'name': 'name',
                'stateMachineArn': 'XXX'
            }]
        }

        machine = StateMachine('name')

        machine.delete()

        self.assertEqual(machine.arn, None)

        calls = [
            mock.call.list_state_machines(),
            mock.call.delete_state_machine(stateMachineArn = 'XXX')
        ]
        self.assertEqual(client.mock_calls, calls)

    @mock.patch('heaviside.create_session', autospec=True)
    def test_delete_exception(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')

        client = iSession.client('stepfunctions')
        client.list_state_machines.return_value = {
            'stateMachines':[]
        }

        machine = StateMachine('name')

        with self.assertRaises(Exception):
            machine.delete(True)

        self.assertEqual(machine.arn, None)

        calls = [
            mock.call.list_state_machines(),
        ]
        self.assertEqual(client.mock_calls, calls)

    @mock.patch('heaviside.create_session', autospec=True)
    def test_start(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')

        client = iSession.client('stepfunctions')
        client.list_state_machines.return_value = {
            'stateMachines':[{
                'name': 'name',
                'stateMachineArn': 'XXX'
            }]
        }

        client.start_execution.return_value = {
            'executionArn': 'YYY'
        }

        machine = StateMachine('name')

        arn = machine.start({}, 'run')

        self.assertEqual(arn, 'YYY')

        calls = [
            mock.call.list_state_machines(),
            mock.call.start_execution(stateMachineArn = 'XXX',
                                      name = 'run',
                                      input = '{}')

        ]
        self.assertEqual(client.mock_calls, calls)

    @mock.patch('heaviside.create_session', autospec=True)
    def test_start_doesnt_exist(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')

        client = iSession.client('stepfunctions')
        client.list_state_machines.return_value = {
            'stateMachines':[]
        }

        machine = StateMachine('name')

        with self.assertRaises(Exception):
            machine.start({}, 'run')

        calls = [
            mock.call.list_state_machines(),
        ]
        self.assertEqual(client.mock_calls, calls)

    @mock.patch('heaviside.create_session', autospec=True)
    def test_stop(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')

        client = iSession.client('stepfunctions')
        client.list_state_machines.return_value = {
            'stateMachines':[{
                'name': 'name',
                'stateMachineArn': 'XXX'
            }]
        }

        machine = StateMachine('name')
        machine.stop('arn', 'error', 'cause')

        calls = [
            mock.call.list_state_machines(),
            mock.call.stop_execution(executionArn = 'arn',
                                     error = 'error',
                                     cause = 'cause')

        ]
        self.assertEqual(client.mock_calls, calls)

    @mock.patch('heaviside.create_session', autospec=True)
    def test_status(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')

        client = iSession.client('stepfunctions')
        client.list_state_machines.return_value = {
            'stateMachines':[{
                'name': 'name',
                'stateMachineArn': 'XXX'
            }]
        }

        client.describe_execution.return_value = {
            'status': 'status'
        }

        machine = StateMachine('name')
        status = machine.status('arn')

        self.assertEqual(status, 'status')

        calls = [
            mock.call.list_state_machines(),
            mock.call.describe_execution(executionArn = 'arn')

        ]
        self.assertEqual(client.mock_calls, calls)

    @mock.patch('heaviside.time.sleep', autospec=True)
    @mock.patch('heaviside.create_session', autospec=True)
    def test_wait_success(self, mCreateSession, mSleep):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')

        client = iSession.client('stepfunctions')
        client.list_state_machines.return_value = {
            'stateMachines':[{
                'name': 'name',
                'stateMachineArn': 'XXX'
            }]
        }

        client.describe_execution.side_effect = [
            {'status': 'RUNNING'},
            {'status': 'SUCCESS', 'output': '{}'}
        ]

        machine = StateMachine('name')
        output = machine.wait('arn')

        self.assertEqual(output, {})

        calls = [
            mock.call.list_state_machines(),
            mock.call.describe_execution(executionArn = 'arn'),
            mock.call.describe_execution(executionArn = 'arn')
        ]
        self.assertEqual(client.mock_calls, calls)

        calls = [
            mock.call(10)
        ]
        self.assertEqual(mSleep.mock_calls, calls)

    @mock.patch('heaviside.time.sleep', autospec=True)
    @mock.patch('heaviside.create_session', autospec=True)
    def _test_wait_xxx(self, error, mCreateSession, mSleep):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')

        client = iSession.client('stepfunctions')
        client.list_state_machines.return_value = {
            'stateMachines':[{
                'name': 'name',
                'stateMachineArn': 'XXX'
            }]
        }

        client.describe_execution.side_effect = [
            {'status': 'RUNNING'},
            {'status': error}
        ]

        client.get_execution_history.return_value = {
            'events':[{
                'execution{}EventDetails'.format(error): {}
            }]
        }

        machine = StateMachine('name')

        if error is None:
            with self.assertRaises(Exception):
                machine.wait('arn')
        else:
            output = machine.wait('arn')
            self.assertEqual(output, {})

        calls = [
            mock.call.list_state_machines(),
            mock.call.describe_execution(executionArn = 'arn'),
            mock.call.describe_execution(executionArn = 'arn'),
            mock.call.get_execution_history(executionArn = 'arn',
                                            reverseOrder = True)
        ]
        self.assertEqual(client.mock_calls, calls)

        calls = [
            mock.call(10)
        ]
        self.assertEqual(mSleep.mock_calls, calls)

    def test_wait_failed(self):
        self._test_wait_xxx('Failed')

    def test_wait_aborted(self):
        self._test_wait_xxx('Aborted')

    def test_wait_timedout(self):
        self._test_wait_xxx('TimedOut')

    def test_wait_exception(self):
        self._test_wait_xxx(None)

    @mock.patch('heaviside.create_session', autospec=True)
    def test_running_arns(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')

        client = iSession.client('stepfunctions')
        client.list_state_machines.return_value = {
            'stateMachines':[{
                'name': 'name',
                'stateMachineArn': 'XXX'
            }]
        }

        client.list_executions.return_value = {
            'executions': [
                {'executionArn': 'arn1'},
                {'executionArn': 'arn2'}
            ]
        }

        machine = StateMachine('name')
        arns = machine.running_arns()

        self.assertEqual(arns, ['arn1', 'arn2'])

        calls = [
            mock.call.list_state_machines(),
            mock.call.list_executions(stateMachineArn = 'XXX',
                                      statusFilter = 'RUNNING')

        ]
        self.assertEqual(client.mock_calls, calls)


