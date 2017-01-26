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

class State(dict):
    """Base class for all States"""

    def __init__(self, name, type_, next=None, end=None, catches=None, retries=None):
        """
        Args:
            name (string): Name of the state
            type_ (string): Type of the State
            next (string|State): Next state to transition to
            end = (boolean): If this state is a terminal state
            catches (Catch|[Catch]): Catch(s) for the State
            retires (Retry|[Retry]): Retry(s) for the State
        """
        super(State, self).__init__(Type = type_)
        self.Name = name

        if next is not None:
            self['Next'] = str(next)

        if end is not None:
            self['End'] = bool(end)

        if catches is not None:
            if type(catches) != list:
                catches = [catches]
            self['Catch'] = catches

        if retries is not None:
            if type(retries) != list:
                retries = [retries]
            self['Retry'] = retries

    def addCatch(self, catch):
        """Add a Catch to this State"""
        if 'Catch' not in self:
            self['Catch'] = []
        self['Catch'].add(catch)

    def addRetry(self, retry):
        """Add a Retry to this State"""
        if 'Retry' not in self:
            self['Retry'] = []
        self['Retry'].add(retry)

    def __str__(self):
        return self.Name

class PassState(State):
    def __init__(self, name, **kwargs):
        """
        Args:
            name (string): Name of the state
            kwargs (dict): Arguments passed to State constructor
        """
        super(PassState , self).__init__(name, 'Pass', **kwargs)

class SuccessState(State):
    def __init__(self, name, **kwargs):
        """
        Args:
            name (string): Name of the state
            kwargs (dict): Arguments passed to State constructor
        """
        super(SuccessState, self).__init__(name, 'Succeed', **kwargs)

class FailState(State):
    def __init__(self, name, error, cause, **kwargs):
        """
        Args:
            name (string): Name of the state
            error (string): Failure error name
            cause (string): Failure error cause
            kwargs (dict): Arguments passed to State constructor
        """
        super(FailState, self).__init__(name, 'Fail', **kwargs)
        self['Error'] = error
        self['Cause'] = cause

class TaskState(State):
    def __init__(self, name, resource, **kwargs):
        """
        Args:
            name (string): Name of the state
            resource (string): ARN of AWS resource to invoke
            kwargs (dict): Arguments passed to State constructor
        """
        super(TaskState, self).__init__(name, 'Task', **kwargs)
        self['Resource'] = resource

class WaitState(State):
    def __init__(self, name, seconds=None, timestamp=None, timestamp_path=None, seconds_path=None, **kwargs):
        """
        Args:
            name (string): Name of the state
            seconds (int): Number of seconds to wait
            timestamp (string|Timestamp): Timestamp to wait until
            timestamp_path (string): JsonPath to location of timestamp
            seconds_path (string): JsonPath to location of seconds
            kwargs (dict): Arguments passed to State constructor
        """
        super(WaitState, self).__init__(name, 'Wait', **kwargs)

        if seconds is not None:
            self['Seconds'] = int(seconds)

        if timestamp is not None:
            self['Timestamp'] = str(timestamp)

        if timestamp_path is not None:
            self['TimestampPath'] = str(timestamp_path)

        if seconds_path is not None:
            self['SecondsPath'] = str(seconds_path)

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
        super(NoteChoice, self).__init__(Not = value)

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

class Retry(dict):
    """A retry statement that reruns a state if the given error(s) are encountered"""

    def __init__(self, errors, interval, max, backoff):
        """
        Args:
            errors (string|list): Error(s) to match against
            interval (int): Initial delay (seconds) before retrying state
            max (int): Max number to retries to attempt
            backoff (int|float): Backoff rate applied to interval after each retry
        """
        if type(errors) != list:
            errors = [errors]

        super(Retry, self).__init__(ErrorEquals = errors,
                         IntervalSeconds = int(interval),
                         MaxAttempts = int(max),
                         BackoffRate = float(backoff))

class Catch(dict):
    """A catch statement that executes a state when the given error(s) are encountered"""

    def __init__(self, errors, next, results=None):
        """
        Args:
            errors (string|list): Error(s) to match against
            next (string|State): State to execute if a matched error is encountered
            results (string|None): ResultPath for error information
        """
        if type(errors) != list:
            errors = [errors]

        super(Catch, self).__init__(ErrorEquals = errors,
                         Next = str(next))

        if results:
            self['ResultPath'] = results

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

