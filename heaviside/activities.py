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

import time
import random
import json
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

        if isinstance(output, Mapping):
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
    def __init__(self, worker, token, target=None, **kwargs):
        super().__init__(name=worker)
        self.token = token # DP ???: move to run method?
        self.target = target
        self.session, self.account_id = create_session(**kwargs)
        self.client = self.session.client('stepfunctions')

    def process(self, input_):
        if self.target:
            return self.target(input_)
        else:
            raise Exception("No target to handle processing")

    def run(self, input_):
        try:
            output_ = self.process(input_)
            self.success(output_)
        except ActivityError as e:
            self.failure(e.error, e.msg)
        except Exception as e:
            self.failure('Unhandled', str(e))

    def success(self, output):
        """Marks the task successfully complete and returns the processed data

        Args:
            output (string|dict): Json response to return to the state machine
        """
        if self.token is None:
            raise Exception("Not currently working on a task")

        if isinstance(output, Mapping):
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

class HeartbeatTaskProcess(TaskProcess):
    def run(self, input_):
        try:
            it = self.process(input_)
            try:
                while True:
                    next(it)
                    self.heartbeat()
            except StopIteration as e:
                output_ = e.value
                self.success(output_)
        except ActivityError as e:
            self.failure(e.error, e.msg)
        except Exception as e:
            self.failure('Unhandled', str(e))

    def heartbeat(self):
        """Sends a heartbeat for states that require heartbeats of long running Activities"""
        if self.token is None:
            raise Exception("Not currently working on a task")

        resp = self.client.send_task_heartbeat(taskToken = self.token)

class ActivityProcess(Process):
    def __init__(self, name, task_proc, **kwargs):
        super().__init__(name=name)
        self.name = name
        self.session, self.account_id = create_session(**kwargs)
        self.client = self.session.client('stepfunctions')

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
                token, input_ = self.activity.task(worker)
                if token is not None:
                    proc = self.task_proc()
                    proc.start()

                    worker = get_worker()
                    try:
                        output_ = self.handle(input_)
                        self.activity.success(output_)
                    except ActivityError as e:
                        self.activity.failure(e.error, e.msg)
                    except Exception as e:
                        self.activity.failure('Unhandled', str(e))
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

class HeartbeatActivityProcess(ActivityProcess):
    def handle(self, input_):
        it = self.handler(input_)
        try:
            while True:
                next(it)
                self.activity.heartbeat()
        except StopIteration as e:
            output_ = e.value
            return output_

def heatbeat_handler(input_):
    for key in input_:
        yield # send heartbeat
        # process input_[key] item
    return {}

class ActivityManager(object):
    def __init__(self):
        self.is_running = False

    def build(self):
        """
        {
            (cls, args [,kwargs]): [cls instances]
        }
        """
        return {}

    def run(self):
        activities = self.build()
        if len(activities) == 0:
            raise Exception("No activities to manage")

        for key in activities:
            for activity in activities[key]:
                activity.start()

        def rebuild(cls, args, kwargs=None):
            if kwargs is not None:
                return cls(*args, **kwargs)
            else:
                return cls(*args)

        self.is_running = True
        while self.is_running:
            time.sleep(60)
            for key in activities:
                alive, dead = [], []
                for activity in activities[key]:
                    (alive if activity.is_alive() else dead).append(activity)
                for activity in dead:
                    alive.append(rebuild(*key))
                activities[key] = alive

def activity_handler(function):
    def wrapper(client, token, input_):
        try:
            output_ = function(input_)
            client.activity_success(token, output_)
        except ActivityError as e:
            client.activity_failure(token, e.error, e.msg)
        except Exception as e:
            client.activity_failure(token, 'Unhandled', str(e))

def heartbeat_handler(generator):
    def wrapper(client, token, input_):
        try:
            it = generator(input_)
            try:
                while True:
                    next(it)
                    client.activity_heartbeat(token)
            except StopIteration as e:
                output_ = e.value
                client.activity_success(token, output_)
        except ActivityError as e:
            client.activity_failure(token, e.error, e.msg)
        except Exception as e:
            client.activity_failure(token, 'Unhandled', str(e))
