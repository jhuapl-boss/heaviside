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

from __future__ import print_function, unicode_literals

import sys
import time
import json
from datetime import datetime

from botocore.exceptions import ClientError

from .lexer import tokenize_source
from .parser import parse
from .exceptions import CompileError, HeavisideError
from .utils import create_session, read

def create_translate(region, account_id):
    """Create translation function that will generate the full ARN URI from a
   given Lambda or Activity name.

    Note: region / account_id can be None if those parts of the ARN are already
          specified

    Args:
        region (string) : The region name for the ARN
        account_id (string) : The account id for the ARN

    Returns:
        function : translate function that can be passed to compile()
    """
    def _translate(type_, function):
        """Common implementation of a translate function the fills out the
        whole ARN

        Args:
            type_ (string): Lambda | Activity
            function (string): Name of Lambda / Activity

        Returns:
            string: ARN URI of the Lambda / Activity
        """
        if type_ == "Lambda":
            defaults = ['arn', 'aws', 'lambda', region, account_id, 'function']
        else:
            defaults = ['arn', 'aws', 'states', region, account_id, 'activity']

        parts = function.split(':')
        name = parts.pop()
        offset = len(defaults) - len(parts)
        try:
            # Wrap the join because an error should only be produced if we try
            # to use a None value. None can be passed in the defaults if that
            # default value is never used
            vals = defaults[:offset]
            vals.extend(parts)
            vals.append(name)
            arn = ":".join(vals)
            return arn
        except TypeError:
            raise TypeError("One or more of the default values for {} ARN '{}' was not specified".format(type_, function))
    return _translate

def compile(source, translate=None, **kwargs):
    """Compile a source step function dsl file into the AWS state machine definition

    Args:
        source (string|Path|file object): Source of step function dsl, passed to read()
        translate (None|function): Function that translates a Lambda / Activity name before
                                   the ARN is completed
        kwargs (dict): Arguments to be passed to json.dumps() when creating the definition

    Returns:
        string: State machine definition
    """
    try:
        with read(source) as fh:
            if hasattr(fh, 'name'):
                source_name = fh.name
            else:
                source_name = "<unknown>"
            tokens = tokenize_source(fh.readline)

        if translate is None:
            translate = lambda x, y: y

        machine = parse(tokens, translate)
        def_ = machine.definition(**kwargs)
        return def_
    except CompileError as e:
        e.source = source_name
        raise e # DP ???: Should the original stacktrace be perserved?
    #except Exception as e:
    #    print("Unhandled Error: {}".format(e))

class StateMachine(object):
    """Class for working with and executing AWS Step Function State Machines"""

    def __init__(self, name, **kwargs):
        """
        Args:
            name (string): Name of the state machine
            kwargs (dict): Same arguments as create_session()
        """
        self.name = name
        self.arn = None
        self.session, self.account_id = create_session(**kwargs)
        self.region = self.session.region_name
        self.client = self.session.client('stepfunctions')
        self._translate = create_translate(self.region, self.account_id)

        resp = self.client.list_state_machines()
        for machine in resp['stateMachines']:
            if machine['name'] == name:
                self.arn = machine['stateMachineArn']
                break

    def build(self, source, **kwargs):
        """Build the state machine definition from a source (file)

        Region and account id are determined from constructor arguments

        Args:
            source (string|file path|file object): Source of step function dsl
            kwargs (dict): Arguments to be passed to json.dumps() when creating the definition

        Returns:
            string: State machine definition

        Raises:
            CompileError: If the was a problem compiling the source
        """
        return compile(source, self._translate, **kwargs)

    def _resolve_role(self, role):
        role = role.strip()
        if not role.lower().startswith("arn:aws:iam"):
            client = self.session.client('iam')
            try:
                response = client.get_role(RoleName=role)
                role = response['Role']['Arn']
            except:
                raise HeavisideError("Could not lookup role '{}'".format(role))

        return role

    def create(self, source, role):
        """Create the state machine in AWS from the give source

        If a state machine with the given name already exists an exception is thrown

        Args:
            source (string|file path|file object): Source of step function dsl
            role (string): AWS IAM role for the state machine to execute under

        Raises:
            CompileError: If the was a problem compiling the source
        """
        if self.arn is not None:
            raise HeavisideError("State Machine {} already exists".format(self.arn))

        role = self._resolve_role(role)
        definition = self.build(source)

        resp = self.client.create_state_machine(name = self.name,
                                                definition = definition,
                                                roleArn = role)

        self.arn = resp['stateMachineArn']

    def delete(self, exception=False):
        """Delete the state machine from AWS

        Args:
            exception (boolean): If an excpetion should be thrown if the machine doesn't exist (default: False)
        """
        if self.arn is None:
            if exception:
                raise HeavisideError("State Machine {} doesn't exist yet".format(self.name))
        else:
            resp = self.client.delete_state_machine(stateMachineArn = self.arn)
            self.arn = None

    def start(self, input_, name=None):
        """Start executing the state machine

        If the state machine doesn't exists an exception is thrown

        Args:
            input_ (Json): Json input data for the first state to process
            name (string|None): Name of the execution (default: Name of the state machine)

        Returns:
            string: ARN of the state machine execution, used to get status and output data
        """
        if self.arn is None:
            raise HeavisideError("State Machine {} doesn't exist yet".format(self.name))

        input_ = json.dumps(input_)

        if name is None:
            name = self.name + "-" + datetime.now().strftime("%Y%m%d%H%M%s%f")

        resp = self.client.start_execution(stateMachineArn = self.arn,
                                           name = name,
                                           input = input_)

        arn = resp['executionArn']
        return arn # DP NOTE: Could store ARN in internal dict and return execution name

    def stop(self, arn, error, cause):
        """Stop an execution of the state machine

        Args:
            arn (string): ARN of the execution to stop
            error (string): Error for the stop
            cause (string): Error cause for the stop
        """
        resp = self.client.stop_execution(executionArn = arn,
                                          error = error,
                                          cause = cause)

    def status(self, arn):
        """Get the status of an execution

        Args:
            arn (string): ARN of the execution to get the status of

        Returns:
            string: One of 'RUNNING', 'SUCCEEDED', 'FAILED', 'TIMED_OUT', 'ABORTED'
        """
        resp = self.client.describe_execution(executionArn = arn)
        return resp['status']

    def wait(self, arn, period=10):
        """Wait for an execution to finish and get the results

        Args:
            arn (string): ARN of the execution to get the status of
            period (int): Number of seconds to sleep between polls for status

        Returns:
            dict: Dict of Json data

        Exceptions:
            HeavisideError: If there was an error getting the failure message
        """
        while True:
            resp = self.client.describe_execution(executionArn = arn)
            if resp['status'] != 'RUNNING':
                if 'output' in resp:
                    return json.loads(resp['output'])
                else:
                    resp = self.client.get_execution_history(executionArn = arn,
                                                             reverseOrder = True)
                    event = resp['events'][0]
                    for key in ['Failed', 'Aborted', 'TimedOut']:
                        key = 'execution{}EventDetails'.format(key)
                        if key in event:
                            return event[key]
                    raise HeavisideError("Could not locate error output for execution '{}'".format(arn))
            else:
                time.sleep(period)

    def running_arns(self):
        """Query for the ARNs of running executions

        Returns:
            list: List of strings containing the ARNs of all running executions
        """
        resp = self.client.list_executions(stateMachineArn = self.arn,
                                           statusFilter = 'RUNNING')
        return [ex['executionArn'] for ex in resp['executions']]

