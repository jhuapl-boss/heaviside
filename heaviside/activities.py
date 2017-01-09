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
import time
import random
import json
from collections import Mapping
from string import ascii_uppercase as CHARS
from multiprocessing import Process

from .exceptions import ActivityError
from .utils import create_session

class Activity(object):
    """Class for work with and being an AWS Step Function Activity"""

    def __init__(self, name, arn=None, worker=None, **kwargs):
        """
        Args:
            name (string): Name of the Activity
            arn (string): ARN of the Activity (None to have it looked up)
            worker (string): Name of the worker receiving tasks (None to have one created)
            kwargs (dict): Same arguments as create_session()
        """
        self.name = name
        self.arn = arn
        self.worker = worker or (name + "-" + "".join(random.sample(CHARS, 6)))
        self.token = None
        self.session, self.account_id = create_session(**kwargs)
        self.client = self.session.client('stepfunctions')

        if self.arn is None:
            resp = self.client.list_activities()
            for activity in resp['activities']:
                if activity['name'] == name:
                    self.arn = activity['activityArn']
                    break
        else:
            try:
                resp = self.client.describe_activity(activityArn = self.arn)
                if resp['name'] != name:
                    raise Exception("Name of {} is not {}".format(self.arn, self.name))
            except ClientError:
                raise Exception("ARN {} is not valid".format(self.arn))

    @property
    def exists(self):
        """If the Activity exist (has an ARN in AWS)"""
        return self.arn is not None

    def create(self, exception = False):
        """Create the Activity in AWS

        Args:
            exception (boolean): If an exception should be raised if the Activity already exists (default: False)
        """
        if self.exists:
            if exception:
                raise Exception("Activity {} already exists".format(self.name))
        else:
            resp = self.client.create_activity(name = self.name)
            self.arn = resp['activityArn']

    def delete(self, exception = False):
        """Delete the Activity from AWS

        Args:
            exception (boolean): If an exception should be raised if the Activity doesn't exists (default: False)
        """
        if not self.exists:
            if exception:
                raise Exception("Activity {} doesn't exist".format(self.name))

        resp = self.client.delete_activity(activityArn = self.arn)
        self.arn = None

    def task(self):
        """Query to see if a task exists for processing.

        This methods uses a long poll, waiting up to 60 seconds before returning

        Returns:
            dict|None: Json dictionary of arguments or None if there is no task yet
        """
        if self.token is not None:
            raise Exception("Currently working on a task")

        resp = self.client.get_activity_task(activityArn = self.arn,
                                             workerName = self.worker)

        if len(resp['taskToken']) == 0:
            return None
        else:
            self.token = resp['taskToken']
            return json.loads(resp['input'])

    def success(self, output):
        """Marks the task successfully complete and returns the processed data

        Args:
            output (string|dict): Json response to return to the state machine
        """
        if self.token is None:
            raise Exception("Not currently working on a task")

        if isinstance(output, Mapping) or isinstance(output, list):
            output = json.dumps(output)
        elif not isinstance(output, str):
            raise Exception("Unknown output format")

        resp = self.client.send_task_success(taskToken = self.token,
                                             output = output)
        self.token = None # finished with task

    def failure(self, error, cause):
        """Marks the task as a failure with a given reason

        Args:
            error (string): Failure error
            cause (string): Failure error cause
        """
        if self.token is None:
            raise Exception("Not currently working on a task")

        resp = self.client.send_task_failure(taskToken = self.token,
                                             error = error,
                                             cause = cause)
        self.token = None # finished with task

    def heartbeat(self):
        """Sends a heartbeat for states that require heartbeats of long running Activities"""
        if self.token is None:
            raise Exception("Not currently working on a task")

        resp = self.client.send_task_heartbeat(taskToken = self.token)

class TaskProcess(Process):
    def __init__(self, worker, token, input_, target=None, **kwargs):
        super().__init__(name=worker)
        self.worker = worker
        self.token = token
        self.input_ = input_
        self.target = target
        self.session, self.account_id = create_session(**kwargs)
        self.client = self.session.client('stepfunctions')

    def process(self, input_):
        if self.target:
            return self.target(input_)
        else:
            raise Exception("No target to handle processing")

    def run(self):
        try:
            output_ = self.process(self.input_)
            # DP TODO: Add coroutine support
            if isinstance(output_, types.GeneratorType):
                try:
                    it = output_
                    while True:
                        next(it)
                        self.heartbeat()
                except StopIteration as e:
                    output_ = e.value
            self.success(output_)
        except ActivityError as e:
            self.failure(e.error, e.msg)
        except Exception as e:
            if self.is_timeout(e): # ClientError
                return # Eat timeout error from heartbeat

            self.failure('Unhandled', str(e))

    def is_timeout(self, ex, op_name=None):
        try:
            rst = ex.response['Error']['Code'] == 'TaskTimedOut'
            if op_name:
                rst &= ex.operation_name == op_name
            return rst
        except:
            return False

    def success(self, output):
        """Marks the task successfully complete and returns the processed data

        Args:
            output (string|dict): Json response to return to the state machine
        """
        if self.token is None:
            raise Exception("Not currently working on a task")

        if isinstance(output, Mapping) or isinstance(output, list):
            output = json.dumps(output)
        elif not isinstance(output, str):
            raise Exception("Unknown output format")

        try:
            resp = self.client.send_task_success(taskToken = self.token,
                                                 output = output)
        except ClientError as e:
            # eat the timeout
            if not self.is_timeout(e):
                raise

        self.token = None # finished with task

    def failure(self, error, cause):
        """Marks the task as a failure with a given reason

        Args:
            error (string): Failure error
            cause (string): Failure error cause
        """
        if self.token is None:
            raise Exception("Not currently working on a task")

        try:
            resp = self.client.send_task_failure(taskToken = self.token,
                                                 error = error,
                                                 cause = cause)
        except ClientError as e:
            # eat the timeout
            if not self.is_timeout(e):
                raise

        self.token = None # finished with task

    def heartbeat(self):
        """Sends a heartbeat for states that require heartbeats of long running Activities"""
        if self.token is None:
            raise Exception("Not currently working on a task")

        resp = self.client.send_task_heartbeat(taskToken = self.token)
        # heartbeat error handled in the run() method

class ActivityProcess(Process):
    def __init__(self, name, task_proc, **kwargs):
        super().__init__(name=name)
        self.name = name
        self.credentials = kwargs
        self.session, self.account_id = create_session(**kwargs)
        self.client = self.session.client('stepfunctions')
        self.arn = None

        resp = self.client.list_activities()
        for activity in resp['activities']:
            if activity['name'] == name:
                self.arn = activity['activityArn']
                break
        
        if isinstance(task_proc, str):
            import importlib
            module, _, function = task_proc.rpartition('.')
            module = importlib.import_module(module)
            task_proc = module.__dict__[function]
        self.task_proc = task_proc

    def run(self):
        self.create()

        while True:
            get_worker = lambda: (self.name + '-' + ''.join(random.sample(CHARS,6)))

            worker = get_worker()
            try:
                token, input_ = self.task(worker)
                if token is not None:
                    proc = self.task_proc(worker, token, input_, **self.credentials)
                    proc.start()
                    # DP TODO: figure out how to limit the total number of currently running task processes

                    worker = get_worker()
            except Exception as e:
                pass # DP TODO: figure out logging to syslog

    def create(self, exception = False):
        """Create the Activity in AWS

        Args:
            exception (boolean): If an exception should be raised if the Activity already exists (default: False)
        """
        if self.arn is not None:
            if exception:
                raise Exception("Activity {} already exists".format(self.name))
        else:
            resp = self.client.create_activity(name = self.name)
            self.arn = resp['activityArn']

    def delete(self, exception = False):
        """Delete the Activity from AWS

        Args:
            exception (boolean): If an exception should be raised if the Activity doesn't exists (default: False)
        """
        if self.arn is None:
            if exception:
                raise Exception("Activity {} doesn't exist".format(self.name))

        resp = self.client.delete_activity(activityArn = self.arn)
        self.arn = None

    def task(self, worker):
        """Query to see if a task exists for processing.

        This methods uses a long poll, waiting up to 60 seconds before returning

        Args:
            worker (string): Name of the worker process to receive tasking for

        Returns:
            string|None, dict|None: Task token, Json dictionary of arguments
                                    Nones if there is no task yet
        """
        if self.arn is None:
            raise Exception("Activity {} doesn't exist".format(self.name))

        resp = self.client.get_activity_task(activityArn = self.arn,
                                             workerName = worker)

        if len(resp['taskToken']) == 0:
            return None, None
        else:
            return resp['taskToken'], json.loads(resp['input'])

class ActivityManager(object):
    def __init__(self):
        self.is_running = False

    def build(self):
        """[() -> Process]"""
        return []

    def run(self):
        activities = self.build()
        if len(activities) == 0:
            raise Exception("No activities to manage")

        self.is_running = True

        procs = {}
        for a in activities:
            procs[a] = a()
            procs[a].start()

        while self.is_running:
            time.sleep(60)
            for key in procs:
                if not procs[key].is_alive():
                    procs[key] = key()
                    procs[key].start()

