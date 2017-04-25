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
import logging
from datetime import datetime
from string import ascii_uppercase as CHARS
from multiprocessing import Process

from botocore.exceptions import ClientError
from botocore.client import Config

from .exceptions import ActivityError, HeavisideError
from .utils import create_session

class SFN(object):
    class Status(object):
        __slots__ = ('resp')

        def __init__(self, resp):
            self.resp = resp

        @property
        def failed(self):
            return self.resp['status'] == 'FAILED'

        @property
        def success(self):
            return self.resp['status'] == 'SUCCEEDED'

        @property
        def output(self):
            return json.loads(self.resp['output']) if 'output' in self.resp else None

    def __init__(self, session, name):
        self.client = session.client('stepfunctions')
        self.arn = None

        resp = self.client.list_state_machines()
        for machine in resp['stateMachines']:
            arn = machine['stateMachineArn']
            if arn.endswith(name):
                self.arn = arn
                break

        if self.arn is None:
            raise HeavisideError("Could not find stepfunction {}".format(name))

    @staticmethod
    def create_name():
        return datetime.now().strftime("%Y%m%d%H%M%s%f")

    def launch(self, args):
        resp = self.client.start_execution(stateMachineArn = self.arn,
                                           name = self.create_name(),
                                           input = json.dumps(args))
        return resp['executionArn']

    def status(self, arn):
        resp = self.client.describe_execution(executionArn = arn)
        return SFN.Status(resp)

    def error(self, arn):
        resp = self.client.get_execution_history(executionArn = arn,
                                                 reverseOrder = True)
        event = resp['events'][0]
        for key in ['Failed', 'Aborted', 'TimedOut']:
            key = 'execution{}EventDetails'.format(key)
            if key in event:
                return ActivityError(event[key]['error'], event[key]['cause'])
        return HeavisideError("Unknown exception occurred in sub-process")

def fanout(session, sub_sfn, sub_args, max_concurrent=50, rampup_delay=15, rampup_backoff=0.8, poll_delay=5, status_delay=1):

    log = logging.getLogger(__name__)
    sfn = SFN(session, sub_sfn)

    running = []
    results = []
    delay = rampup_delay
    started_all = False

    if not isinstance(sub_args, types.GeneratorType):
        sub_args = (arg for arg in sub_args) # convert to a generator

    while True:
        try:
            while not started_all and len(running) < max_concurrent:
                running.append(sfn.launch(next(sub_args)))
                time.sleep(delay)
                if delay > 0:
                    delay = int(delay * rampup_backoff)
        except StopIteration:
            started_all = True
            log.debug("Finished launching sub-processes")

        time.sleep(poll_delay)

        still_running = []
        for arn in running:
            status = sfn.status(arn)
            if status.failed:
                error = sfn.error(arn)
                log.debug("Sub-process failed: {}".format(error))
                raise error
            elif status.success:
                results.append(status.output)
            else:
                still_running.append(arn)
            time.sleep(status_delay)

        log.debug("Sub-processes finished: {}".format(len(running) - len(still_running)))
        log.debug("Sub-processes running: {}".format(len(still_running)))
        running = still_running

        if started_all and len(running) == 0:
            log.debug("Finished")
            break

    return results

class TaskMixin(object):
    """
    Expects:
        self.log (Logger) : used to log status information
        self.process (Function) : function used to process task input
                                  returns either the results or a generator
                                  (for sending heartbeat messages)
        self.client (Boto3.Client) : Client for stepfunctions, used to communicate
                                     with AWS

    Uses:
        self.token (String|None) : The token for the currently processing task,
                                   will be set by handle_task()
    """

    def __init__(self, process=lambda x: x, **kwargs):
        """Will not be called if used as a mixin. Provides just the expected variables."""
        session, _ = create_session(**kwargs)
        self.client = session.client('stepfunctions')
        self.log = logging.getLogger(__name__)
        self.process = process
        self.token = None

    def handle_task(self, token, input_):
        """Called by Process.start() to process data

        Note: If the task times out, the task results (or running coroutine)
              are silently discarded.
        """

        if self.token is not None and self.token != token:
            raise Exception("Currently working on a task")

        self.token = token

        try:
            output_ = self.process(input_)
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
            self.log.exception("ActivityError caught")
            self.failure(e.error, e.cause)
        except Exception as e:
            if self.is_timeout(e): # ClientError
                self.token = None
                return # Eat timeout error from heartbeat

            error = type(e).__name__
            self.log.exception("{} caught".format(error))
            self.failure(error, str(e))

    @staticmethod
    def resolve_function(task_proc):
        import importlib
        module, _, function = task_proc.rpartition('.')
        module = importlib.import_module(module)
        task_proc = module.__dict__[function]
        return task_proc

    @staticmethod
    def is_timeout(ex, op_name=None):
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
                self.log.exception("Error sending task success")
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
                self.log.exception("Eror sending task failure")
                raise
        finally:
            self.token = None # finished with task

    def heartbeat(self):
        """Sends a heartbeat for states that require heartbeats of long running Activities"""
        if self.token is None:
            raise Exception("Not currently working on a task")

        resp = self.client.send_task_heartbeat(taskToken = self.token)
        # heartbeat error handled in the handle_task() method
        # DP TODO: put error handling here, as heartbeat may be used outside of handle_task()

class ActivityMixin(object):
    """
    Expects:
        self.log (Logger) : used to log status information
        self.client (Boto3.Client) : Client for stepfunctions, used to communicate
                                     with AWS
        self.arn (String) : If Step Function ARN or Name is not passed to self.run()
                            Will be set/overridden by self.run(name=ARN) if provided
        self.handle_task (Function) : function used to process task input and send
                                      success or failure message. If return is not
                                      None, then a Thread like object is expected.
                                      The object's start() method will be called.
        self.max_concurrent (int) : Used if self.handle_task returns a non-None
                                    result to limit the number of concurrent executing
                                    workers. Zero denotes an unlimited number of
                                    concurrent executions
        self.poll_delay (int) : Seconds between polling for worker's status if the
                                maximum number of concurrent processes has been reached

    Uses:
        self.name (String) : (OPTIONAL) The name of the activity (the last part of the ARN)
        self.workers (list): Used if self.handle_task returns a non-None result to
                             store the concurrently executing workers. Expects objects
                             to have an is_alive() method to know when execution has
                             finished and it can be cleaned up.
    """

    def __init__(self, handle_task = lambda t, i: None, **kwargs):
        """Will not be called if used as a mixin. Provides just the expected variables."""
        session, _ = create_session(**kwargs)
        # DP NOTE: read_timeout is needed so that the long poll for tasking doesn't
        #          timeout client side before AWS returns that there is no work
        self.client = session.client('stepfunctions', config=Config(read_timeout=70))
        self.log = logging.getLogger(__name__)
        self.arn = None
        self.handle_task = handle_task
        self.max_concurrent = 0
        self.poll_delay = 1

    def lookup_activity_arn(self, name):
        resp = self.client.list_activities()
        for activity in resp['activities']:
            if activity['name'] == name:
                return activity['activityArn']

    def create_worker_name(self, name = None):
        rand = ''.join(random.sample(CHARS, 6))

        if name is None:
            if hasattr(self, 'name'):
                name = self.name

        if name is not None:
            rand = name + '-' + rand

        return rand

    def run(self, name=None):
        if name is None:
            # DP TODO: look for self.name as well
            if not hasattr(self, 'arn') or self.arn is None:
                raise Exception("Could not locate Activity ARN to poll")
            name = self.arn.split(':')[-1]
        else:
            if name.lower().startswith("arn:"):
                self.arn = name
            else:
                self.arn = self.lookup_activity_arn(name)
                self.create_activity(name)

        self.workers = []

        self.log.debug("Starting polling {} for tasking".format(self.arn))
        # DP NOTE: needed for unit test, so the loop will exit
        # DP TODO: make thread safe so it can be used to stop a running thread
        self.polling = True
        while self.polling:
            try:
                worker = self.create_worker_name(name)
                token, input_ = self.poll_task(worker)
                if token is not None:
                    try:
                        results = self.handle_task(token, input_) # DP ???: What to do with worker name?

                        # DP TODO: seperate error handling for concurrent processes from handle_task
                        if results is not None:
                            results.start()
                            # DP TODO: Need a check to make sure the subprocess/thread starts
                            #          There can be a problem if the subprocess/thread doesn't start
                            #          or get to its own error handling logic
                            # ???: Maybe store workers as token : subprocess, allowing us to always try to fail
                            #      if a given flag is not set or is false
                            self.workers.append(results)

                            if self.max_concurrent > 0:
                                while len(self.workers) >= self.max_concurrent:
                                    workers = []
                                    # remove any finished (not alive) workers
                                    for worker in self.workers:
                                        if worker.is_alive():
                                            workers.append(worker)
                                    self.workers = workers
                                    if len(self.workers) >= self.max_concurrent:
                                        time.sleep(self.poll_delay)
                    except Exception as e:
                        self.log.exception("Problem launching task process")
                        self.log.debug("Attempting to fail task")
                        try:
                            self.client.send_task_failure(taskToken = token,
                                                          error = 'Heaviside.Unknown',
                                                          cause = str(e))
                        except:
                            self.log.exception("Couldn't fail task")
                            pass # Eat error. Either a problem talking with AWS or task was already finished

                    # DP ???: create a list of launched processes, so on terminate the handling tasks will terminate too
                    #         if so, need to figure out how to send a failure for the tasks that havn't finished...
            except KeyboardInterrupt:
                self.log.info("CTRL-C caught, terminating")
            except Exception as e:
                # DP ???: create a flag for if a task was accepted and fail it if there was an issue launching the task
                # DP ???: What to do when there is an exception communicating with AWS
                #         Stop running, wait, just loop and continue to fail?
                self.log.exception("Problem getting tasking")

    def poll_task(self, worker = None):
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

        if worker is None:
            worker = self.create_worker_name()

        resp = self.client.get_activity_task(activityArn = self.arn,
                                             workerName = worker)

        if 'taskToken' not in resp or len(resp['taskToken']) == 0:
            return None, None
        else:
            return resp['taskToken'], json.loads(resp['input'])

    def create_activity(self, name = None, exception = False):
        """Create the Activity in AWS

        Args:
            exception (boolean): If an exception should be raised if the Activity already exists (default: False)
        """
        if self.arn is not None:
            if exception:
                raise Exception("Activity {} already exists".format(self.arn))
        else:
            if name is None:
                if hasattr(self, 'name'):
                    name = self.name
            if name is None:
                raise Exception("No activity name given")

            resp = self.client.create_activity(name = name)
            self.arn = resp['activityArn']

    def delete_activity(self, exception = False):
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


class Activity(ActivityMixin, TaskMixin):
    def __init__(self, name, arn=None, worker=None, **kwargs):
        self.name = name
        self.arn = arn
        self.worker = worker
        self.token = None

        self.session, self.account_id = create_session(**kwargs)
        self.client = self.session.client('stepfunctions', config=Config(read_timeout=70))
        self.log = logging.getLogger(__name__)

        self.max_concurrent = 0
        self.poll_delay = 1

        if self.arn is None:
            self.arn = self.lookup_activity_arn(name)
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

class TaskProcess(Process, TaskMixin):
    """Process for processing an Activity's task input"""

    def __init__(self, token, input_, target=None, **kwargs):
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
        super(TaskProcess, self).__init__(name=token)
        self.token = token
        self.input_ = input_
        self.target = target
        self.session, self.account_id = create_session(**kwargs)
        self.client = self.session.client('stepfunctions')
        self.log = logging.getLogger(__name__)

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
        try:
            self.handle_task(self.token, self.input_)
        except Exception as e:
            self.log.exception("Problem in TaskProcess")

            if self.token is not None:
                self.log.info("Atempting to fail task")
                error = type(e).__name__
                self.failure(error, str(e))

class ActivityProcess(Process, ActivityMixin):
    """Process for polling an Activity's ARN and launching TaskProcesses to handle
    the given tasking
    """

    def __init__(self, name, target=None, **kwargs):
        """
        Note: task_proc arguments are the same as TaskProcess (minus the target argument)
        Note: task_proc must return an object with a start() method

        Args:
            name (string): Name of the activity to monitor
                           The activity's ARN is looked up in AWS using the provided
                           AWS credentials
            target (string|callable): Function to pass to TaskProcess as the target,
                                      If string, the class / function will be imported
            kwargs (dict): Same arguments as utils.create_session()
        """
        super(ActivityProcess, self).__init__(name=name)
        self.name = name
        self.credentials = kwargs
        self.session, self.account_id = create_session(**kwargs)
        self.client = self.session.client('stepfunctions', config=Config(read_timeout=70))
        self.log = logging.getLogger(__name__)

        self.max_concurrent = 0
        self.poll_delay = 1

        if isinstance(target, str):
            target = TaskProcess.resolve_function(target)
        self.target = target

    def handle_task(self, token, input_):
        return TaskProcess(token, input_, target=self.target, **self.credentials)

    def run(self):
        # NOTE: The default implementation of run() in Process hides the ActivityMixin version
        try:
            ActivityMixin.run(self, name=self.name)
        except:
            self.log.exception("Problem in ActivityProcess")

class ActivityManager(object):
    """Manager for launching multiple ActivityProcesses and monitoring that
    they are still running.

    Designed to be subclasses and customized to provide a list of processes to start
    and monitor.
    """

    def __init__(self, **kwargs):
        self.is_running = False
        self.log = logging.getLogger(__name__)
        self.activities = {}
        self.credentials = kwargs

    def build(self, activity):
        return ActivityProcess(activity,
                               target = self.activities[activity],
                               **self.credentials)

    def run(self):
        """Start the initial set of processes and monitor them to ensure
        they are still running. If any have stopped, the method will restart
        the processes.

        Note: This is a blocking method. It will continue to run until
        self.is_running is False
        """
        if len(self.activities) == 0:
            raise Exception("No activities to manage")

        self.log.info("Starting initial Activity workers")
        self.is_running = True

        procs = {}
        try:
            for activity in self.activities:
                procs[activity] = self.build(activity)
                procs[activity].start()

            self.log.info("Finished starting initial Activity workers")

            while self.is_running:
                for key in procs:
                    if not procs[key].is_alive():
                        self.log.debug("Activity worker {} died, restarting".format(key))

                        procs[key] = self.build(key)
                        procs[key].start()
                time.sleep(60)
        except KeyboardInterrupt:
            self.log.info("CTRL-C caught, terminating Activity workers")

            for key in procs:
                if procs[key].is_alive():
                    procs[key].terminate()
        except:
            self.log.exception("Unexpected exception caught, terminating manager")

