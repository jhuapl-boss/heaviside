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
from copy import deepcopy

from boto3.session import Session
from botocore.exceptions import ClientError
from botocore.client import Config

from .exceptions import ActivityError, HeavisideError
from .utils import create_session

class SFN(object):
    """Wrapper for Boto3 StepFunction calls and response parsing"""

    class Status(object):
        """Wrapper for StepFunction status response"""

        __slots__ = ('resp')

        def __init__(self, resp):
            self.resp = resp

        @property
        def failed(self):
            """If the StepFunction's state is FAILED"""
            return self.resp['status'] == 'FAILED'

        @property
        def success(self):
            """If the StepFunction's state is SUCCEEDED"""
            return self.resp['status'] == 'SUCCEEDED'

        @property
        def output(self):
            """The parsed JSON output of the StepFunction"""
            return json.loads(self.resp['output']) if 'output' in self.resp else None

        def __str__(self):
            return "{}: {}".format(self.resp['status'], self.output)

    def __init__(self, session, name, have_full_arn=False):
        """
        Args:
            session (boto3.session) : Active session used to communicate
            name (string) : Name of the Step Function to execute
                            Used to lookup the full ARN
            have_full_arn (bool) : True if name is the full ARN, so lookup not
                                   required
        """
        self.client = session.client('stepfunctions')
        self.arn = None

        if have_full_arn:
            self.arn = name
        else:
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
        """Helper method to generate a unique execution name"""
        return '{}-{}'.format(datetime.now().strftime("%Y%m%d%H%M%s%f"), random.randint(0,9999))

    def launch(self, args):
        """Execute the named StepFunction with the given arguments

        Args:
            args : Argument to be passed to the StepFunction
                   Will be JSON encoded before being sent

        Returns:
            String : Execution ARN
        """
        resp = self.client.start_execution(stateMachineArn = self.arn,
                                           name = self.create_name(),
                                           input = json.dumps(args))
        return resp['executionArn']

    def status(self, arn):
        """Get the status of the given execution

        Args:
            arn (String) : ARN of execution to query for status

        Returns:
            SFN.Status : Status information and execution output (if successful)
        """
        resp = self.client.describe_execution(executionArn = arn)
        return SFN.Status(resp)

    def error(self, arn):
        """Get an exception containing the error and reason for a failed execution

        Args:
            arn (String) : ARN of execution to query for error information

        Returns:
            ActivityError : With the error and cause, if it can be determined
                            or a generic error message
        """
        resp = self.client.get_execution_history(executionArn = arn,
                                                 reverseOrder = True)
        event = resp['events'][0]
        for key in ['Failed', 'Aborted', 'TimedOut']:
            key = 'execution{}EventDetails'.format(key)
            if key in event:
                return ActivityError(event[key]['error'], event[key]['cause'])
        return ActivityError("Heaviside.Unknown", "Unknown exception occurred in sub-process")

    def cancel(self, arn):
        """Cancel the given execution

        Args:
            arn (string): ARN of the execution to cancel
        """
        resp = self.client.stop_execution(executionArn = arn,
                                          error = "Heaviside.Fanout",
                                          cause = "Sub-process error detected")

def fanout(session, sub_sfn, sub_args, max_concurrent=50, rampup_delay=15, rampup_backoff=0.8, poll_delay=5, status_delay=1, common_sub_args={}):
    """Activity helper method for executing a dynamic number of StepFunctions
    and returning the results.

    Executes the sub_sfn for each sub_args element. If any of the sub_sfn
    executions fail, the error information is located and an ActivityError
    is raised.

    NOTE: Currently all state is maintained in memory, if there is an exception
          fanout will attempt to stop all currently executing sub_sfn. Any
          problems stopping currently executing sub_sfn will be logged.
    NOTE: Currently fanout works best with stateless / idempotent sub_sfn or
          sub_sfn where the caller can cleanup state in the case of an error.
    NOTE: AWS polling limits are a 200 unit bucket per account, refilling at 1 unit
          per second. The arguments max_concurent, poll_delay, and status_delay
          can be used to limit the AWS request rate of fanout to a rate that
          will not exceed the AWS polling limits

    Args:
        session (boto3.session) : Active session for communicating with AWS
        sub_sfn (String) : Name or full ARN of StepFunction to execute
                           Used to locate the full ARN in AWS
        sub_args (list) : Arguments for each sub_sfn execution
                          Each element is passed to a different
                          execution of the sub_sfn
        max_concurrent (int) : Total number of concurrently executing sub_sfn
                               that can be launched. Once this limit is hit
                               fanout waits until some current executions have
                               finished before launching more sub_sfn.
        rampup_delay (int) : Seconds
                             Initial delay between launches of sub_sfn. Allows
                             for AWS resources to detect and scale to the large
                             volume of requests that can be generated.
        rampup_backoff (float) : Multiplier for rampup_delay that reduces the
                                 delay until there is no delay left between
                                 launches of sub_sfn.
        poll_delay (int) : Seconds
                           Delay between launching (multiple) sub_sfn and polling
                           for their status.
        status_delay (int) : Seconds
                             Delay between polling the status of each concurrently
                             executing sub_sfn. Used to limit AWS API request
                             speed of fanout and not run into throttling problems.
        common_sub_args (dict) : Common arguments that should be passed to each
                                 step function

    Returns:
        list : A list of the JSON parsed results for each executed sub_sfn.

    Exceptions:
        ActivityError : If there was a failure in a sub_sfn execution, contains
                        the failure error and cause if it can be determined.
    """
    args = {
        'sub_sfn': sub_sfn,
        'common_sub_args': common_sub_args,
        'sub_args': list(sub_args), # Handle generator type that could previous be passed
        'max_concurrent': max_concurrent,
        'rampup_delay': rampup_delay,
        'rampup_backoff': rampup_backoff,
        'status_delay': status_delay,
        'finished': False,
        'running': [],
        'results': []
    }

    while True:
        args = fanout_nonblocking(args, session)

        if args['finished']:
            return args['results']

        time.sleep(poll_delay)

def fanout_nonblocking(args, session=None):
    """Helper method for executing a dynamic number of StepFunctions and
    returning the results.

    This is a non-blocking version of fanout, designed to be executed by
    a Lambda in a StepFunction loop, where the loop handles the poll_delay
    sleep. The function is designed so the output is the input for the next
    iteration of the polling loop.

    Args:
        args: {
            # See fanout for full details of arguments
            sub_sfn (ARN)
            sub_sfn_is_full_arn (bool) : optional.  True if full arn supplied for sub_sfn
            common_sub_args (dict) : Common arguments that should be passed to each
                                     step function
            sub_args (list)
            max_concurrent (int)
            rampup_delay (int)
            rampup_backoff (float)
            status_delay (int)

            running (list) : List of running ARNs that are being polled
            results (list) : List of JSON parsed results for each executed sub_sfn
            finished (boolean) : Status flag that will be set to True when
                                 all sub_sfn have been launched and have finished
        }
        session (boto3.session|None): Active session for communicating with AWS
                                      or None to construct one from the environment

    Returns:
        dict : An updated copy of the args dict

    Exceptions:
        ActivityError
    """
    log = logging.getLogger(__name__)

    sub_sfn = args['sub_sfn']
    sub_args = args['sub_args']
    common_sub_args = args['common_sub_args']
    max_concurrent = args['max_concurrent']
    rampup_delay = args['rampup_delay']
    rampup_backoff = args['rampup_backoff']
    status_delay = args['status_delay']

    have_full_arn = False
    if 'sub_sfn_is_full_arn' in args:
        if args['sub_sfn_is_full_arn']:
            have_full_arn = True

    if session is None:
        session = Session()
    sfn = SFN(session, sub_sfn, have_full_arn)

    running = args['running']
    results = args['results']
    handling_exception = True # Detect if we need to stop all running executions

    try:
        # Check the status of all running sub_sfn executions
        still_running = list(running)
        error = None
        for arn in running:
            status = sfn.status(arn)
            if status.failed:
                log.debug("Sub-process failed: {}".format(error))
                still_running.remove(arn)
                if error is None: # Don't care if there are multiple errors
                    error = sfn.error(arn)
            elif status.success:
                still_running.remove(arn)
                results.append(status.output)

            time.sleep(status_delay) # Slow the request rate

        log.debug("Sub-processes finished: {}".format(len(running) - len(still_running)))
        log.debug("Sub-processes running: {}".format(len(still_running)))
        running = args['running'] = still_running

        if error is not None:
            raise error

        # Launch any remaining sub_sfn, as max_concurrent allows
        while len(sub_args) > 0 and len(running) < max_concurrent:
            sfn_inputs = sub_args[0]
            # Merge common arguments with specific sub_args.
            if isinstance(sub_args[0], dict):
                # Use copy instead since we're about to mutate.
                sfn_inputs = deepcopy(sub_args[0])
                sfn_inputs.update(common_sub_args)
            elif len(common_sub_args) > 0:
                log.warning("common_sub_args ignored because sub_args is not a dict")
            try:
                running.append(sfn.launch(sfn_inputs))
                sub_args.pop(0)
            except sfn.client.exceptions.ExecutionAlreadyExists:
                # Don't kill running step functions if accidentally reuse name
                handling_exception = False
                # No need to report this exception to the step function's task
                return args

            except ClientError as ex:
                # Don't kill running step functions when throttled
                if ex.response["Error"]["Code"] == "ThrottlingException":
                    handling_exception = False
                elif ex.response["Error"]["Code"] == "TooManyRequestsException":
                    handling_exception = False

                # Let exception bubble up
                raise

            if rampup_delay > 0:
                time.sleep(rampup_delay)
                rampup_delay = args['rampup_delay'] = int(rampup_delay * rampup_backoff)

        if len(sub_args) == 0 and len(running) == 0:
            args['finished'] = True

        handling_exception = False
        return args

    finally:
        if handling_exception:
            # DP NOTE: Using a finally clause instead of except because of the
            #          differences in how exceptions need to be reraised between
            #          Python 2 and 3. This will keep the traceback untouched
            for arn in running:
                try:
                    sfn.cancel(arn)
                except:
                    log.exception("Could not cancel {}".format(arn))

class TaskMixin(object):
    """Mixin with helper methods for processing and sending results back

    Expects:
        self.log (Logger) : used to log status information
        self.process (Function) : function used to process task input
                                  returns either the results or a generator
                                  (for sending heartbeat messages)
        self.client (boto3.Client) : Client for stepfunctions, used to communicate
                                     with AWS

    Uses:
        self.token (String|None) : The token for the currently processing task,
                                   will be set by handle_task()
    """

    def __init__(self, process=lambda x: x, **kwargs):
        """Will not be called if used as a mixin. Provides just the expected variables.
        
        Args:
            process (callable) : Callable to handle task input and return output or
                                 generator (for sending heartbeat messages)
            kwargs : Arguments for heaviside.utils.create_session
        """
        session, _ = create_session(**kwargs)
        self.client = session.client('stepfunctions')
        self.log = logging.getLogger(__name__)
        self.process = process
        self.token = None

    def handle_task(self, token, input_):
        """Wrapper around process() that handles sending heartbeats and results
        and will handle exceptions and send a failure.
        
        Note: If the task times out, the task results (or running coroutine)
              are silently discarded.

        Args:
            token (String) : Task token
            input_ : JSON parsed input
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
        """Import the given function module and return the function callable

        Args:
            task_proc (string) : module and function name

        Returns:
            callable : Referenced function
        """
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
    """Mixin with helper methods for polling for Activity tasking and spawing
    wokers.

    Expects:
        self.log (Logger) : used to log status information
        self.client (Boto3.Client) : Client for stepfunctions, used to communicate
                                     with AWS
        self.name (String) : If Step Function ARN or Name is not passed to self.run()
                             Will be set/overridden by self.run(name=ARN) if provided
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
        self.name (String) : The name of the activity (the last part of the ARN)
        self.workers (list): Used if self.handle_task returns a non-None result to
                             store the concurrently executing workers. Expects objects
                             to have an is_alive() method to know when execution has
                             finished and it can be cleaned up.
        self.polling (boolean) : Flag used to control main loop in run()
    """

    def __init__(self, handle_task = lambda t, i: None, **kwargs):
        """Will not be called if used as a mixin. Provides just the expected variables.
        
        Args:
            handle_task (callable) : Callable to process task input and send success or
                                     failure
            kwargs : Arguments for heaviside.utils.create_session
        """
        session, _ = create_session(**kwargs)
        # DP NOTE: read_timeout is needed so that the long poll for tasking doesn't
        #          timeout client side before AWS returns that there is no work
        self.client = session.client('stepfunctions', config=Config(read_timeout=70))
        self.log = logging.getLogger(__name__)
        self.name = None
        self.arn = None
        self.handle_task = handle_task
        self.max_concurrent = 0
        self.poll_delay = 1
        self.polling = False

    def lookup_activity_arn(self, name):
        """Locate the ARN of the given activity name"""
        resp = self.client.list_activities()
        for activity in resp['activities']:
            if activity['name'] == name:
                return activity['activityArn']
        return None

    def create_worker_name(self):
        """Generate a unique worker name"""
        rand = ''.join(random.sample(CHARS, 6))

        if self.name is not None:
            rand = self.name + '-' + rand

        return rand

    def run(self, name=None):
        """Start polling for Activity tasking

        Args:
            name (String|None) : Name or ARN of the Activity to monitor, if arn
                                 is not already defined
        """
        if name is None:
            if not hasattr(self, 'arn') or self.arn is None:
                raise Exception("Could not locate Activity ARN to poll")

            self.name = self.arn.split(':')[-1]
        else:
            if name.lower().startswith("arn:"):
                self.arn = name
                self.name = self.arn.split(':')[-1]
            else:
                self.arn = self.lookup_activity_arn(name)
                self.name = name
                self.create_activity()

        # Storage for currently executing workers
        self.workers = []

        self.log.debug("Starting polling {} for tasking".format(self.arn))
        # DP NOTE: needed for unit test, so the loop will exit
        # DP TODO: make thread safe so it can be used to stop a running thread
        self.polling = True
        while self.polling:
            try:
                worker = self.create_worker_name()
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
                self.polling = False
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
                             If None, a name will be generated

        Returns:
            string|None, dict|None: Task token, Json dictionary of arguments
                                    Nones if there is no task yet
        """
        if self.arn is None:
            name = self.name if hasattr(self, 'name') and self.name is not None else '<Unknown>'
            raise Exception("Activity {} doesn't exist".format(name))

        if worker is None:
            worker = self.create_worker_name()

        resp = self.client.get_activity_task(activityArn = self.arn,
                                             workerName = worker)

        if 'taskToken' not in resp or len(resp['taskToken']) == 0:
            return None, None
        else:
            return resp['taskToken'], json.loads(resp['input'])

    def create_activity(self, exception = False):
        """Create the Activity in AWS

        Args:
            exception (boolean): If an exception should be raised if the Activity already exists (default: False)
        """
        if self.arn is not None:
            if exception:
                raise Exception("Activity {} already exists".format(self.arn))
        else:
            if not hasattr(self, 'name') or self.name is None:
                raise Exception("No activity name given")

            resp = self.client.create_activity(name = self.name)
            self.arn = resp['activityArn']

    def delete_activity(self, exception = False):
        """Delete the Activity from AWS

        Args:
            exception (boolean): If an exception should be raised if the Activity doesn't exists (default: False)
        """
        if self.arn is None:
            if exception:
                name = self.name if hasattr(self, 'name') and self.name is not None else '<Unknown>'
                raise Exception("Activity {} doesn't exist".format(name))
        else:
            resp = self.client.delete_activity(activityArn = self.arn)
            self.arn = None


class Activity(ActivityMixin, TaskMixin):
    """Implementation of both ActivityMixin and TaskMixin in a single object
    with a constructor that configures the expected variables.
    """

    def __init__(self, name, arn=None, worker=None, **kwargs):
        """
        Args:
            name (String): Name of the Activity to monitor
            arn (String): Full ARN of Activity to monitor
                          If not given, it is looked up
                          If given, the actual ARN and Name are compared
            process (callable): Callable that transforms the task's input
                                into an output that is then returned
            kwargs : Arguments to heaviside.utils.create_session
        """
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

