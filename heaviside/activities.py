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
from string import ascii_uppercase as CHARS
from multiprocessing import Process

from botocore.exceptions import ClientError

from .exceptions import ActivityError
from .utils import create_session

# DP TODO: rewrite as Mixins so that Processes, Threads, and other implementations can be used

class Activity(object):
    """Class for work with and being an AWS Step Function Activity"""

    def __init__(self, name, arn=None, worker=None, **kwargs):
        """
        Args:
            name (string): Name of the Activity
            arn (string): ARN of the Activity (None to have it looked up)
            worker (string): Name of the worker receiving tasks (None to have one created)
            kwargs (dict): Same arguments as utils.create_session()
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

        output = json.dumps(output)

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
    """Process for processing an Activity's task input"""

    def __init__(self, worker, token, input_, target=None, **kwargs):
        """
        Args:
            worker (string): Name of the activity work that accepted the tasking
            token (string): AWS StepFunction token associated with the accepted tasking
            input_ (Json): Json input to the activity invocation
            target (callable|None): Function to call to process the input_ data
                                    If target returns a generator corouting a heartbeat
                                    is sent each time the function yields control
            kwargs (dict): Same arguments as utils.create_session()
        """
        super(TaskProcess, self).__init__(name=worker)
        self.worker = worker
        self.token = token
        self.input_ = input_
        self.target = target
        self.session, self.account_id = create_session(**kwargs)
        self.client = self.session.client('stepfunctions')

    def process(self, input_):
        """Call the target function or raise an exception.

        Designed to be overriden by a child class to provide an easy hook for processing
        data.

        Args:
            input_ (Json): Input Json data passed to the constructor

        Returns:
            Generator Coroutine : This activity wants a heartbeat to be sent each time
                                  the coroutine yields control
            object : The results of processing the input data, to be passed to success()
        """
        if self.target:
            return self.target(input_)
        else:
            # DP ???: Use ActivityError('Unhandled', ...)???
            raise Exception("No target to handle processing")

    def run(self):
        """Called by Process.start() to process data

        Note: If the task times out, the task results (or running coroutine)
              are silently discarded.
        """
        try:
            output_ = self.process(self.input_)
            # DP TODO: Add coroutine support
            if isinstance(output_, types.GeneratorType):
                try:
                    it = output_
                    while True:
                        output_ = next(it)
                        self.heartbeat()
                except StopIteration as e:
                    # Support getting the output from both Python 2 & 3 generators
                    if hasattr(e, 'value'):
                        output_ = e.value
            self.success(output_)
        except ActivityError as e:
            self.failure(e.error, e.cause)
        except Exception as e:
            if self.is_timeout(e): # ClientError
                self.token = None
                return # Eat timeout error from heartbeat

            error = type(e).__name__
            self.failure(error, str(e))

    def is_timeout(self, ex, op_name=None):
        """Check the exception to determine if it is a Boto3 ClientError
        thrown because the task timed out.

        Args:
            ex (Exception) : Exception to check
            op_name (string|None) : (Optional) name of the operation that was attempted

        Returns:
            bool : If the exception was caused by the task timing out
        """
        try:
            rst = ex.response['Error']['Code'] == 'TaskTimedOut'
            if op_name:
                rst &= ex.operation_name == op_name
            return rst
        except:
            return False

    def success(self, output):
        """Marks the task successfully complete and returns the processed data

        Note: This method will silently fail if the task has timed out

        Args:
            output (string|dict): Json response to return to the state machine
        """
        if self.token is None:
            raise Exception("Not currently working on a task")

        output = json.dumps(output)

        try:
            resp = self.client.send_task_success(taskToken = self.token,
                                                 output = output)
        except ClientError as e:
            # eat the timeout
            if not self.is_timeout(e):
                raise
        finally:
            self.token = None # finished with task

    def failure(self, error, cause):
        """Marks the task as a failure with a given reason

        Note: This method will silently fail if the task has timed out

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
        finally:
            self.token = None # finished with task

    def heartbeat(self):
        """Sends a heartbeat for states that require heartbeats of long running Activities"""
        if self.token is None:
            raise Exception("Not currently working on a task")

        resp = self.client.send_task_heartbeat(taskToken = self.token)
        # heartbeat error handled in the run() method

class ActivityProcess(Process):
    """Process for polling an Activity's ARN and launching TaskProcesses to handle
    the given tasking
    """

    def __init__(self, name, task_proc, **kwargs):
        """
        Note: task_proc arguments are the same as TaskProcess (minus the target argument)
        Note: task_proc must return an object with a start() method

        Args:
            name (string): Name of the activity to monitor
                           The activity's ARN is looked up in AWS using the provided
                           AWS credentials
            task_proc (string|callable): Callable that produces a Process to run
                                         If string, the class / function will be imported
            kwargs (dict): Same arguments as utils.create_session()
        """
        super(ActivityProcess, self).__init__(name=name)
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
        """Called by Process.start() to process data

        Note: Automatically creates the Activity ARN if it doesn't exist

        """
        self.create()

        get_worker = lambda: (self.name + '-' + ''.join(random.sample(CHARS,6)))

        worker = get_worker()

        # Note: needed for unit test, so the loop will exit
        self.running = True
        while self.running:
            try:
                token, input_ = self.task(worker)
                if token is not None:
                    proc = self.task_proc(worker, token, input_, **self.credentials)
                    proc.start()
                    # DP TODO: figure out how to limit the total number of currently running task processes
                    # DP ???: create a list of launched processes, so on terminate the handling tasks will terminate too
                    #         if so, need to figure out how to send a failure for the tasks that havn't finished...

                    worker = get_worker()
            except Exception as e:
                # DP ???: create a new worker name?
                # DP ???: create a flag for if a task was accepted and fail it if there was an issue launching the task
                # DP ???: What to do when there is an exception communicating with AWS
                #         Stop running, wait, just loop and continue to fail?
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
        else:
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
    """Manager for launching multiple ActivityProcesses and monitoring that
    they are still running.

    Designed to be subclasses and customized to provide a list of processes to start
    and monitor.
    """

    def __init__(self):
        self.is_running = False

    def build(self):
        """Build a list of callables that return a process to monitor

        Each callable is called once to create the inital process and
        then each time the current process dies.

        Should be overridden by the subclass.

        Returns:
            List of callables
        """
        return []

    def run(self):
        """Start the initial set of processes and monitor them to ensure
        they are still running. If any have stopped, the method will restart
        the processes.

        Note: This is a blocking method. It will continue to run until
        self.is_running is False
        """
        activities = self.build()
        if len(activities) == 0:
            raise Exception("No activities to manage")

        self.is_running = True

        procs = {}
        try:
            for a in activities:
                procs[a] = a()
                procs[a].start()

            while self.is_running:
                for key in procs:
                    if not procs[key].is_alive():
                        procs[key] = key()
                        procs[key].start()
                time.sleep(60)
        except KeyboardInterrupt:
            for key in procs:
                if procs[key].is_alive():
                    procs[key].terminate()

