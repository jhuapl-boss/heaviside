# Copyright 2020 The Johns Hopkins University Applied Physics Laboratory
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

from __future__ import unicode_literals

import json
from collections import OrderedDict

import iso8601 # parser for timestamp format

from .ast import ASTStateChoice, ASTCompOp, ASTCompNot, ASTCompAndOr, ASTValue, ASTModNext, ASTStepFunction

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

    def __repr__(self):
        return "Timestamp({!r})".format(self.timestamp)

class _StateMachineEncoder(json.JSONEncoder):
    """Custom JSONEncoder that handles the Timestamp type"""
    def default(self, o):
        if type(o) == Timestamp:
            return str(o)
        return super(_StateMachineEncoder, self).default(o)

class Branch(dict):
    def __init__(self, ast):
        super(Branch, self).__init__()

        # Makes states be dumped in the same order they were added
        # making it easier to read the output and match it to the input
        self['States'] = OrderedDict()
        for state in ast.states:
            self['States'][state.name] = State(state)

        self['StartAt'] = ast.states[0].name

class StepFunction(Branch):
    def __init__(self, ast):
        super(StepFunction, self).__init__(ast)

        if ast.comment is not None:
            self['Comment'] = ast.comment.value

        if ast.version is not None:
            self['Version'] = ast.version.value.value

        if ast.timeout is not None:
            self['TimeoutSeconds'] = ast.timeout.value.value

    def definition(self, **kwargs):
        """Dump the state machine into the JSON format needed by AWS

        Args:
            kwargs (dict): Arguments passed to json.dumps()
        """
        kwargs.setdefault('ensure_ascii', False) # Allow Unicode in the output by default

        return json.dumps(self, cls=_StateMachineEncoder, **kwargs)

class State(dict):
    def __init__(self, ast):
        super(State, self).__init__()

        self['Type'] = ast.state_type

        # Generic Modifiers for all States
        if ast.comment is not None:
            # No longer a token, parsed by AST class into name/comment
            self['Comment'] = ast.comment

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
            self['Result'] = ast.data.value

        if ast.catch is not None:
            self['Catch'] = []
            for catch in ast.catch:
                self['Catch'].append(Catch(catch))

        if ast.retry is not None:
            self['Retry'] = []
            for retry in ast.retry:
                self['Retry'].append(Retry(retry))

        if ast.iterator is not None:
            # The iterator contains a separate state machine that runs on each
            # element of the input array.
            substates = [b for b in ast.iterator.states]
            self['Iterator'] = StepFunction(ASTStepFunction(None, None, None, substates))

        if ast.items_path is not None:
            self['ItemsPath'] = ast.items_path.value.value

        if ast.max_concurrency is not None:
            max_con = ast.max_concurrency.value.value
            if max_con < 0:
                ast.max_concurrency.raise_error("max_concurrency must be non-negative")
            self['MaxConcurrency'] = max_con

        # State specific arguments
        if ast.state_type == 'Fail':
            self['Error'] = ast.error.value
            self['Cause'] = ast.cause.value

        if ast.state_type == 'Pass' or ast.state_type == 'Map':
            if ast.parameters is not None:
                self['Parameters'] = Parameters(ast.parameters)

        if ast.state_type == 'Task':
            self['Resource'] = ast.arn
            if ast.parameters is not None:
                self['Parameters'] = Parameters(ast.parameters)

        if ast.state_type == 'Wait':
            key = ''.join([t.capitalize() for t in ast.type.value.split('_')])
            self[key] = ast.val.value

        if ast.state_type == 'Choice':
            key = ASTStateChoice.DEFAULT
            if key in ast.branches:
                self['Default'] = ast.branches[key]
                del ast.branches[key]

            self['Choices'] = []
            for comp in ast.branches:
                self['Choices'].append(Choice(comp, ast.branches[comp]))

        if ast.state_type == 'Parallel':
            self['Branches'] = []
            for branch in ast.branches:
                self['Branches'].append(Branch(branch))

        if ast.next is not None:
            if isinstance(ast.next, ASTModNext):
                self['Next'] = ast.next.value
            else:
                self['Next'] = ast.next

        if ast.end:
            self['End'] = ast.end

class Catch(dict):
    def __init__(self, ast):
        super(Catch, self).__init__()

        errors = ast.errors

        # Support a single string for error type
        # ??? put this transformation in AST
        if type(errors) != list:
            errors = [errors]

        if len(errors) == 0:
            errors = [ASTValue('States.ALL', None)]

        self['ErrorEquals'] = [e.value for e in errors]
        self['Next'] = ast.next

        if ast.path is not None:
            self['ResultPath'] = ast.path.value

class Retry(dict):
    def __init__(self, ast):
        super(Retry, self).__init__()

        errors = ast.errors

        # Support a single string for error type
        # ??? put this transformation in AST
        if type(errors) != list:
            errors = [errors]

        if len(errors) == 0:
            errors = [ASTValue('States.ALL', None)]

        if float(ast.backoff.value) < 1.0:
            ast.backoff.raise_error("Backoff rate should be >= 1.0")

        self['ErrorEquals'] = [e.value for e in errors]
        self['IntervalSeconds'] = ast.interval.value
        self['MaxAttempts'] = ast.max.value
        self['BackoffRate'] = float(ast.backoff.value)

def Parameters(ast):
    rst = OrderedDict()
    for k,v in ast.items():
        # Keys are ASTValues and need to have the actual value unwrapped
        # Values are already raw values as they are JSON Text
        rst[k.value] = v
    return rst

COMPARISON = {
    '==': {
        str: 'StringEquals',
        int: 'NumericEquals',
        float: 'NumericEquals',
        bool: 'BooleanEquals',
        Timestamp: 'TimestampEquals',
    },
    '<': {
        str: 'StringLessThan',
        int: 'NumericLessThan',
        float: 'NumericLessThan',
        Timestamp: 'TimestampLessThan',
    },
    '>': {
        str: 'StringGreaterThan',
        int: 'NumericGreaterThan',
        float: 'NumericGreaterThan',
        Timestamp: 'TimestampGreaterThan',
    },
    '<=': {
        str: 'StringLessThanEquals',
        int: 'NumericLessThanEquals',
        float: 'NumericLessThanEquals',
        Timestamp: 'TimestampLessThanEquals',
    },
    '>=': {
        str: 'StringGreaterThanEquals',
        int: 'NumericGreaterThanEquals',
        float: 'NumericGreaterThanEquals',
        Timestamp: 'TimestampGreaterThanEquals',
    },
}

try:
    for op in COMPARISON.keys():
        COMPARISON[op][unicode] = COMPARISON[op][str]
except NameError:
    pass # Support Python2 Unicode string type

def Choice(ast, target=None):
    if type(ast) == ASTCompOp:
        var = ast.var.value
        val = ast.val.value
        op = ast.op.value
        op_type = type(val) # The type of the operator is based on the value type
        try:
            if op == '!=':
                op = COMPARISON['=='][op_type]
                choice = OpChoice(var, op, val)
                return NotChoice(choice, target)
            else:
                op = COMPARISON[op][op_type]
                return OpChoice(var, op, val, target)
        except KeyError:
            msg = "Cannot make '{}' comparison with type '{}'".format(op, op_type)
            ast.raise_error(msg)
    elif type(ast) == ASTCompNot:
        return NotChoice(Choice(ast.comp), target)
    elif isinstance(ast, ASTCompAndOr):
        return AndOrChoice(ast, target)
    else:
        ast.raise_error("Comparison support not implemented yet")

class OpChoice(dict):
    """A ChoiceState Choice wrapping a comparison and reference to state to execute"""

    def __init__(self, var, op, val, target=None):
        super(OpChoice, self).__init__(Variable = var)

        self.op = op # for __str__ / __repr__
        self[self.op] = val

        if target is not None:
            self['Next'] = target

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "({} {} {})".format(self['Variable'], self.op, self[self.op])

class NotChoice(dict):
    """Wraper around a Choice that negates the Choice"""

    def __init__(self, comp, target=None):
        super(NotChoice, self).__init__(Not = comp)

        if target is not None:
            self['Next'] = target

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "(Not {!r})".format(self['Not'])

class AndOrChoice(dict):
    """Wrapper arounds a list of Choices and 'and's or 'or's the results together"""

    def __init__(self, ast, target=None):
        super(AndOrChoice, self).__init__()

        self.op = ast.op # for __str__ / __repr__
        self[self.op] = [Choice(comp) for comp in ast.comps]

        if target is not None:
            self['Next'] = target

    def __str__(self):
        return repr(self)

    def __repr__(self):
        vals = map(repr, self[self.op])
        return "(" + (" {} ".format(self.op.lower())).join(vals) + ")"

