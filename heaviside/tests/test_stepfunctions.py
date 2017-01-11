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

import os
import sys
import json
import unittest
from unittest import mock

# Allow unit test files to import the target library modules
cur_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.normpath(os.path.join(cur_dir, '..', '..'))
sys.path.append(parent_dir)

from lib.stepfunctions import StateMachine, Activity, compile, create_session

"""
create_session
    Test all of the different types of input both valid and invalid

Activity
    stub out create_session
    stub out random.sample
    test constructor
        self.worker name
        self.arn lookup / verification
    methods
        test guard conditions
        test boto3 call
"""

class MockSession(object):
    def __init__(self):
        super().__init__()

        self.clients = {}

    def client(self, name):
        if name not in self.clients:
            self.clients[name] = mock.MagicMock()
        return self.clients[name]

class TestActivity(unittest.TestCase):
    @mock.patch('lib.stepfunctions.create_session', autospec=True)
    @mock.patch('lib.stepfunctions.random.sample', autospec=True)
    def test_init_no_worker(self, mSample, mCreateSession):
        mSample.return_value = 'XXX'
        mCreateSession.return_value = (MockSession(), '123456')

        activity = Activity('name')

        self.assertEqual(activity.worker, 'name-XXX')

    @mock.patch('lib.stepfunctions.create_session', autospec=True)
    def test_init_worker(self, mCreateSession):
        mCreateSession.return_value = (MockSession(), '123456')

        activity = Activity('name', worker='name')

        self.assertEqual(activity.worker, 'name')

    @mock.patch('lib.stepfunctions.create_session', autospec=True)
    def test_init_doesnt_exist(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')

        iClient = iSession.client('stepfunctions')
        iClient.list_activities.return_value = {
            'activities': [
            ]
        }

        activity = Activity('name')

        call = mock.call.list_activities()
        self.assertEqual(iClient.mock_calls, [call])

    @mock.patch('lib.stepfunctions.create_session', autospec=True)
    def test_init_exists(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')

        iClient = iSession.client('stepfunctions')
        iClient.list_activities.return_value = {
            'activities': [{
                'name': 'name',
                'activityArn': 'XXX'
            }]
        }

        activity = Activity('name')

        call = mock.call.list_activities()
        self.assertEqual(iClient.mock_calls, [call])

        self.assertEqual(activity.arn, 'XXX')

    @mock.patch('lib.stepfunctions.create_session', autospec=True)
    def test_init_lookup(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')

        iClient = iSession.client('stepfunctions')
        iClient.describe_activity.return_value = {
            'name': 'name'
        }

        activity = Activity('name', arn='XXX')

        call = mock.call.describe_activity(activityArn = 'XXX')
        self.assertEqual(iClient.mock_calls, [call])

        self.assertEqual(activity.arn, 'XXX')

    @mock.patch('lib.stepfunctions.create_session', autospec=True)
    def test_init_invalid_lookup(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')

        iClient = iSession.client('stepfunctions')
        iClient.describe_activity.return_value = {
            'name': 'XXX'
        }

        with self.assertRaises(Exception):
            activity = Activity('name', arn='XXX')

        call = mock.call.describe_activity(activityArn = 'XXX')
        self.assertEqual(iClient.mock_calls, [call])

    @mock.patch('lib.stepfunctions.create_session', autospec=True)
    def test_create(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')

        iClient = iSession.client('stepfunctions')
        iClient.list_activities.return_value = {
            'activities': [
            ]
        }
        iClient.create_activity.return_value = {
            'activityArn': 'XXX'
        }

        activity = Activity('name')
        activity.create(True)

        self.assertEqual(activity.arn, 'XXX')

        calls = [
            mock.call.list_activities(),
            mock.call.create_activity(name = 'name')
        ]
        self.assertEqual(iClient.mock_calls, calls)

    @mock.patch('lib.stepfunctions.create_session', autospec=True)
    def test_failed_create(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')

        iClient = iSession.client('stepfunctions')
        iClient.list_activities.return_value = {
            'activities': [{
                'name': 'name',
                'activityArn': 'XXX'
            }]
        }

        activity = Activity('name')
        with self.assertRaises(Exception):
            activity.create(True)

        calls = [
            mock.call.list_activities(),
        ]
        self.assertEqual(iClient.mock_calls, calls)

    @mock.patch('lib.stepfunctions.create_session', autospec=True)
    def test_delete(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')

        iClient = iSession.client('stepfunctions')
        iClient.list_activities.return_value = {
            'activities': [{
                'name': 'name',
                'activityArn': 'XXX'
            }]
        }

        activity = Activity('name')
        activity.delete(True)

        self.assertEqual(activity.arn, None)

        calls = [
            mock.call.list_activities(),
            mock.call.delete_activity(activityArn = 'XXX')
        ]
        self.assertEqual(iClient.mock_calls, calls)

    @mock.patch('lib.stepfunctions.create_session', autospec=True)
    def test_failed_delete(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')

        iClient = iSession.client('stepfunctions')
        iClient.list_activities.return_value = {
            'activities': [
            ]
        }

        activity = Activity('name')
        with self.assertRaises(Exception):
            activity.delete(True)

        calls = [
            mock.call.list_activities(),
        ]
        self.assertEqual(iClient.mock_calls, calls)

    @mock.patch('lib.stepfunctions.create_session', autospec=True)
    def test_task(self, mCreateSession):
        iSession = MockSession()
        mCreateSession.return_value = (iSession, '123456')

        iClient = iSession.client('stepfunctions')
        iClient.list_activities.return_value = {
            'activities': [{
                'name': 'name',
                'activityArn': 'XXX'
            }]
        }
        expected_input = [1,2,3]
        iClient.get_activity_task.return_value = {
            'taskToken': 'XXX',
            'input': json.dumps(expected_input)
        }

        activity = Activity('name', worker='name')
        actual_input = activity.task()

        self.assertEqual(actual_input, expected_input)
        self.assertEqual(activity.token, 'XXX')

        calls = [
            mock.call.list_activities(),
            mock.call.get_activity_task(activityArn = 'XXX', workerName = 'name')
        ]
        self.assertEqual(iClient.mock_calls, calls)

