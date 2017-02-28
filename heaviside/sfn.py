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

import json
from collections import OrderedDict

import iso8601 # parser for timestamp format

class _StateMachineEncoder(json.JSONEncoder):
    """Custom JSONEncoder that handles the Timestamp type"""
    def default(self, o):
        if type(o) == Timestamp:
            return str(o)
        return super(_StateMachineEncoder, self).default(o)

class StepFunction(dict):
    def __init__(self, ast):
        super(StepFunction, self).__init__()
        self['States'] = OrderedDict()
        for state in ast.states:
            self['States'][state.name] = State(state)
        self['StartAt'] = ast.states[0].name

        if ast.comment is not None:
            self['Comment'] = ast.comment

        if ast.version is not None:
            self['Version'] = ast.version

        if ast.timeout is not None:
            self['TimeoutSeconds'] = ast.timeout

    def definition(self, **kwargs):
        """Dump the state machine into the JSON format needed by AWS

        Args:
            kwargs (dict): Arguments passed to json.dumps()
        """
        return json.dumps(self, cls=_StateMachineEncoder, **kwargs)

class State(dict):
    def __init__(self, ast):
        super(State, self).__init__()

        self['Type'] = ast.state_type

        # Generic Modifiers for all States
        if ast.comment is not None:
            self['Comment'] = ast.comment.value.value

        if ast.timeout is not None:
            timeout = ast.timeout.value.value
            self['TimeoutSeconds'] = timeout
        else:
            timeout = 60 # default

        if ast.heartbeat is not None:
            heartbeat = ast.heartbeat.value.value
            if not heartbeat < timeout:
                ast.heartbeat.raise_error("Heartbeat must be less than timeout (defaults to 60)")
            self['HeartbeatSeconds'] = heartbeat

        if ast.input is not None:
            self['InputPath'] = ast.input.value.value

        if ast.result is not None:
            self['ResultPath'] = ast.result.value.value

        if ast.output is not None:
            self['OutputPath'] = ast.output.value.value

        if ast.data is not None:
            self['Result'] = ast.data.value.value

        if ast.catch is not None:
            self['Catch'] = []
            for catch in ast.catch:
                self['Catch'].append(Catch(catch))

        if ast.retry is not None:
            self['Retry'] = []
            for retry in ast.retry:
                self['Retry'].append(Retry(retry))


        # State specific arguments
        if ast.state_type == 'Fail':
            self['Error'] = ast.error.value.value
            self['Cause'] = ast.cause.value.value

        if ast.state_type == 'Task':
            self['Resource'] = ast.arn.value.value

        if ast.state_type == 'Wait':
            key = ''.join([t.Capitalize() for t in ast.type.value.valuesplit('_')])
            self[key] = ast.val.value.value

        if ast.next is not None:
            self['Next'] = ast.next

        if ast.end:
            self['End'] = ast.end

class Catch(dict):
    def __init__(self, ast):
        super(Catch, self).__init__()

        errors = ast.errors.value.value

        # Support a single string for error type
        # ??? put this transformation in AST
        if type(errors) != list:
            errors = [errors]

        self['ErrorEquals'] = errors
        self['Next'] = None # TODO Implement

        if ast.path is not None:
            self['ResultPAth'] = ast.path.value.value

class Retry(dict):
    def __init__(self, ast):
        super(Retry, self).__init__()

        errors = ast.errors.value.value

        # Support a single string for error type
        # ??? put this transformation in AST
        if type(errors) != list:
            errors = [errors]

        self['ErrorEquals'] = errors
        self['IntervalSeconds'] = ast.interval.value.value
        self['MaxAttempts'] = ast.max.value.value
        self['BackoffRate'] = ast.backoff.value.value

class Branch(dict):
    """A branch of execution. A list of self contains states (only references 
    to states within the list) and a pointer to the first state to start
    execution on
    """

    def __init__(self, states=None, start=None):
        """
        Args:
            states (None|list|dict): No States, a list of States, or dict of
                                     States for the branch to execute
            start (None|State|string): The first State to start execution on
        """
        super(Branch, self).__init__()
        # Makes states be dumped in the same order they were added
        # making it easier to read the output and match it to the input
        self['States'] = OrderedDict() 

        if type(states) == list:
            for state in states:
                self.addState(state)
        elif type(states) == dict:
            self['States'].update(states)

        if start is not None:
            self.setStart(start)

    def setStart(self, state):
        """Set the start State

        Args:
            state (State|string): State / Name of State to start execution on
        """
        self['StartAt'] = str(state)

    def addState(self, state):
        """Add a State to the list of States available to execute

        Args:
            state (State): State to add
        """
        self['States'][state.Name] = state


class Machine(Branch):
    """State Machine, consisting of a single branch of execution"""

    def __init__(self, comment=None, states=None, start=None, version=None, timeout=None):
        """
        Args:
            comment (string): State machine comment
            state (list): List of States to execute
            start (State|string): State to start execution on
            version (string): AWS State MAchine Language version used
            timeout (int): Overall state machine timeout
        """
        super(Machine, self).__init__(states, start)

        if comment is not None:
            self['Comment'] = comment

        if version is not None:
            self['Version'] = version

        if timeout is not None:
            self['TimeoutSeconds'] = timeout

    def definition(self, **kwargs):
        """Dump the state machine into the JSON format needed by AWS

        Args:
            kwargs (dict): Arguments passed to json.dumps()
        """
        return json.dumps(self, cls=_StateMachineEncoder, **kwargs)

class ParallelState(State):
    def __init__(self, name, branches=None, **kwargs):
        """
        Args:
            name (string): Name of the state
            branches (list): List of List of States to be executed
            kwargs (dict): Arguments passed to State constructor
        """
        super(ParallelState, self).__init__(name, 'Parallel', **kwargs)

        if branches is None:
            branches = []
        self['Branches'] = branches

    def addBranch(self, branch):
        """Add another branch of execution
        
        Args:
            branch (list): List of States to execute"""
        self['Branches'].add(branch)

class ChoiceState(State):
    def __init__(self, name, choices=None, default=None, **kwargs):
        """
        Args:
            name (string): Name of the state
            choices (Choice|[Choice]): Choice(s) that are checked and
                                       potentially executed
            default (string|State): Default state if no Choice matches
            kwargs (dict): Arguments passed to State constructor
        """
        super(ChoiceState, self).__init__(name, 'Choice', **kwargs)

        if choices is None:
            choices = []
        elif type(choices) != list:
            choices = [choices]
        self['Choices'] = choices

        if default is not None:
            self['Default'] = str(default)

    def addChoice(self, choice):
        """Add another Choice to the list of Choices to compare against"""
        self['Choices'].add(choice)

    def setDefault(self, default):
        """Sets the default State to execute if there is no match"""
        self['Default'] = str(default)

class Choice(dict):
    """A ChoiceState Choice wrapping a comparison and reference to state to execute"""

    def __init__(self, variable, op, value, next_ = None):
        """
        Args:
            variable (string): JsonPath of variable to compare
            op (string): AWS Step Function Language operator name
            value (int|float|string|Timestamp): value to compare against
            next (string|State): State to execute if the comparison is True
        """
        super(Choice, self).__init__(Variable = variable)

        self[op] = value
        self.op = op # for __str__ / __repr__

        if next_ is not None:
            self['Next'] = str(next_)

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "({} {} {})".format(self['Variable'], self.op, self[self.op])

class NotChoice(dict):
    """Wraper around a Choice that negates the Choice"""

    def __init__(self, value, next_ = None):
        """
        Args:
            value (Choice): Choice to negate
            next (string|State): State to execute if the negated comparison is True
        """
        super(NotChoice, self).__init__(Not = value)

        if next_ is not None:
            self['Next'] = str(next_)

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "(Not {!r})".format(self['Not'])

class AndOrChoice(dict):
    """Wrapper arounds a list of Choices and 'and's or 'or's the results together"""

    def __init__(self, op, values, next_ = None):
        """
        Args:
            op (string): 'and' or 'or' depending on which operator is desired
            values (list): list of two or more Choices to combind the results of
            next (string|State): State to execute if the whole results is True
        """
        super(AndOrChoice, self).__init__()

        if type(values) != list:
            values = [values]

        self[op] = values
        self.op = op # for __str__ / __repr__

        if next_ is not None:
            self['Next'] = str(next_)

    def __str__(self):
        return repr(self)

    def __repr__(self):
        vals = map(repr, self[self.op])
        return "(" + (" {} ".format(self.op.lower())).join(vals) + ")"

def _resolve(actual, defaults):
    """Break the actual arn apart and insert the defaults for the
    unspecified begining parts of the arn (based on position in the
    arn)

    Example:
        actual: 'account_id:function:FUNCTION_NAME'
        defaults: ['arn', 'aws', 'lambda', 'region', 'account_id', 'function']
        return: 'arn:aws:lambda:region:account_id:function:FUNCTION_NAME'
    
    Args:
        actual (string): ARN style string, potentially missing part of the
                         begining of the ARN. Must include the final name of
                         the ARN function
        defaults (list): List of ARN components to use to fill in the missing
                         parts of the actual ARN

    Returns:
        (string): Complete ARN
    """
    actual_ = actual.split(':')
    name = actual_.pop()
    offset = len(defaults) - len(actual_)
    try:
        # Wrap the join because an error should only be produced if we try
        # to use a None value. None can be passed in the defaults if that
        # default value is never used
        vals = defaults[:offset]
        vals.extend(actual_)
        vals.append(name)
        arn = ":".join(vals)
        return arn
    except TypeError:
        raise Exception("One or more of the default values for ARN '{}' was not specified".format(actual))


def Lambda(name, region=None, account=None):
    """Resolve a partial Lambda ARN into a full ARN with the given region/account
    
    Args:
        name (string): Partial ARN
        region (string): AWS region of the Lambda function
        account (string): AWS account id owning the Labmda function
    """

    defaults = ['arn', 'aws', 'lambda', region, account, 'function']
    return _resolve(name, defaults)

def Activity(name, region=None, account=None):
    """Resolve a partial Activity ARN into a full ARN with the given region/account
    
    Args:
        name (string): Partial ARN
        region (string): AWS region of the Activity ARN
        account (string): AWS account id owning the Activity ARN
    """

    defaults = ['arn', 'aws', 'states', region, account, 'activity']
    return _resolve(name, defaults)

class Timestamp(object):
    """Wrapper around a timestamp string.

    Used to determine if a string is in a valid timestamp format and type it
    for the parser
    """

    def __init__(self, timestamp):
        """
        Args:
            timestamp (string): Timestamp string

        Exceptions:
            An exception is thrown if the string is not a valid timestamp
        """
        iso8601.parse_date(timestamp)
        self.timestamp = timestamp

    def __str__(self):
        return self.timestamp

