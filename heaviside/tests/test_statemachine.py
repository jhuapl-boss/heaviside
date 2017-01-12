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
from unittest import mock

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
            mock.call(sfn, 'region', '123456', machine._translate)
        ]
        self.assertEqual(mCompile.mock_calls, calls)

