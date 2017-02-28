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

from funcparserlib.parser import (some, a, many, skip, maybe, forward_decl)
from funcparserlib.parser import NoParseError, State

from .lexer import Token
from .exceptions import CompileError
from .ast import *

from .sfn import StepFunction
from .sfn import Machine
from .sfn import Retry, Catch, Timestamp
from .sfn import Lambda, Activity
from .sfn import ChoiceState, Choice, NotChoice, AndOrChoice
from .sfn import ParallelState, Branch

def _link(states, final=None):
    """Take a list of states and links them together in the order given

    Automatically creates a default end state if there is no default and the ChoiceState
    terminates.

    States for ChoiceState branches (and catch blocks) are processed and added to the list
    of returned states.

    Convention: If the State has an attribute 'branches' is should be a list of list of state.
                Each list of state will be linked together and then appended to the list of
                returned States.

    Args:
        states (list): List of States to link together
        final (string|State|None): Name of next state to link the final state to
                                   None to terminate execution with the final state

    Returns:
        list: List of linked States
    """
    linked = []
    for i in range(len(states)):
        state = states[i]

        # DP TODO: check to see if the given state name already exists
        linked.append(state)

        next_ = states[i+1] if i+1 < len(states) else final

        # The first three conditions are checking to see if the state needs
        # to be linked with the next state or if it is already linked / terminates
        if 'Next' in state:
            pass # process branches, if they exist
        elif 'End' in state:
            pass # process branches, if they exist
        elif type(state) in (SuccessState, FailState): #terminal states
            pass
        elif type(state) == ChoiceState:
            if 'Default' not in state:
                next__ = next_ # prevent branches from using the new end state (just use End=True)
                if next__ is None:
                    next__ = SuccessState(str(state) + "Next")
                    linked.append(next__)
                state['Default'] = str(next__)
        else:
            if next_ is None:
                state['End'] = True
            else:
                state['Next'] = str(next_)

        if hasattr(state, 'branches'):
            for branch in state.branches:
                linked.extend(link(branch, final=next_))

    return linked

def make_name(line):
    """Take the line number of a state and return the state name"""
    return "Line{}".format(line)

def add_name_comment(state, comment):
    """Take a doc string comment and set the state name and comment

    Args:
        state (State): State to update the name of and add the comment to
        comment (None|tuple): None or tuple of (name, comment)
                              If name is an empty string no name change is performed
    """
    if comment:
        name, comment = comment.split('\n', 1)

        name = name.strip()
        # DP TODO: Remove indent from each line of the comment, preserving internal indent
        comment = '\n'.join([l.strip() for l in comment.split('\n')])

        if  len(name) > 0:
            if type(state) == ChoiceState:
                # update while loops to use the new name
                if state.branches[0][-1]['Next'] == state.Name:
                    state.branches[0][-1]['Next'] = name
            state.Name = name

        state['Comment'] = comment

# Dictionary of comparison operator and type that resolves
# to the state machine comparison operator
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

def make(cls):
    def make_(args):
        return cls(*args)
    return make_

def const(value):
    def const_(token):
        return ASTValue(value, token)

def tok_to_value(token):
    return ASTValue(token.value, token)

def toktype(code):
    return some(lambda x: x.code == code) >> tok_to_value

def op(operator):
    return a(Token('OP', operator)) >> tok_to_value

def op_(operator):
    return skip(op(operator))

def n(name):
    return a(Token('NAME', name)) >> tok_to_value

def n_(name):
    return skip(n(name))

# Primatives
true = (n('true') | n('True')) >> const(True)
false = (n('false') | n('False')) >> const(False)
boolean = true | false

def value_to_number(ast):
    try:
        ast.value = int(ast.value)
    except ValueError:
        try:
            ast.value = float(ast.value)
        except ValueError:
            ast.raise_error("'{}' is not a valid number".format(ast.value))
    return ast

number = some(lambda x: x.code == 'NUMBER') >> tok_to_value >> value_to_number

def value_to_string(ast):
    if ast.value[:3] in ('"""', "'''"):
        ast.value = ast.value[3:-3]
    else:
        ast.value = ast.value[1:-1]
    return ast

string = some(lambda x: x.code =='STRING') >> tok_to_value >> value_to_string

def string_to_timestamp(ast):
    try:
        ast.value = Timestamp(ast.value)
    except:
        ast.raise_error("'{}' is not a valid timestamp".format(ast.value))

timestamp = string >> string_to_timestamp

end = skip(a(Token('ENDMARKER', '')))
block_s = skip(toktype('INDENT'))
block_e = skip(toktype('DEDENT'))

def debug(x):
    """Print the current object being parsed"""
    print(x)
    return x

def debug_(m):
    """Print the current object with a prefix

    Args:
        m (string): Prefix to print before debuged object
    """
    def debug__(a):
        print("{}: {!r}".format(m, a))
        return a
    return debug__

def make_number(n):
    """Parse the given string as an int or float"""
    try:
        return int(n)
    except ValueError:
        return float(n)

def make_string(n):
    """Remove a strings quotes and return the raw string

    NOTE: escaping is done / undone
    """
    if n[:3] in ('"""', "'''"):
        s = n[3:-3]
    else:
        s = n[1:-1]
    return s

def make_ts_str(s):
    """Check to see if the given string is a Timestamp

    Returns:
        Timestamp|string: Timestamp if the string is a valid timestamp
                          String if the the string is not a timestamp
    """
    try:
        # A timestamp is also a valid string, so it has to be manually parsed
        # instead of using the lexer
        return Timestamp(s)
    except:
        return s

def make_array(n):
    """Take the results of parsing an array and return an array

    Args:
        n (None|list): None for empty list
                       list should be [head, [tail]]
    """
    if n is None:
        return []
    else:
        return [n[0]] + n[1]

def make_object(n):
    """Take a list of pairs and create a dict
    
    NOTE: run through make_array to transform the results to an array
    """
    return dict(make_array(n))

# =============
# Simple States
# =============
def make_pass(args):
    """Make a PassState

    Args:
        args (int): Line number
    """
    line = args

    name = make_name(line)
    state = PassState(name)
    state.line = line
    return state

def make_success(args):
    """Make a SuccessState

    Args:
        args (int): Line number
    """
    line = args

    name = make_name(line)
    state = SuccessState(name)
    state.line = line
    return state

def make_fail(args):
    """Make a FailState

    Args:
        args (tuple): (line number:int, error:string, cause:string)
    """
    line, error, cause = args

    name = make_name(line)
    state = FailState(name, error, cause)
    state.line = line
    return state

# make_task moved into parse function to have access to parse arguments

def make_wait(args):
    """Make a WaitState

    Args:
        args (tuple): (line number:int, key:string, value:string|int|Timestamp)
    """
    line, key, value = args

    if key == 'timestamp' and type(value) != Timestamp:
        raise CompileError(line, 0, '', "Invalid timestamp '{}'".format(value))

    name = make_name(line)
    kwargs = {key: value}

    state = WaitState(name, **kwargs)
    state.line = line
    return state

# ============
# Flow Control
# ============
def make_comp_simple(args):
    """Make a Choice statement for a ChoiceState

    Args:
        args (tuple): (var:string, op:string, val:bool|string|int|Timestamp)
    """
    var, op, val = args
    
    try:
        if op == '!=':
            op = COMPARISON['=='][type(val)]
            choice = Choice(var, op, val)
            return NotChoice(choice)
        else:
            op = COMPARISON[op][type(val)]
            return Choice(var, op, val)
    except KeyError:
        msg = "Cannot make '{}' comparison with type '{}'".format(op, type(val).__name__)
        # TODO figure out location information
        raise CompileError(0, 0, '', msg)

def make_comp_not(args):
    """Wrap the given Choice in a NotChoice"""
    return NotChoice(args)

def make_comp_andor(args):
    """Process a list of and / or combinding Choices into an AndOrChoice

    Args:
        args (tuple): (Choice, [('and'|'or', Choice)])
                      The 'and' or 'or' operator should be
                      the same for all tuples in the list
    """
    x, xs = args

    if len(xs) == 0:
        return x

    vals = [x]
    op = ''
    for op_, val in xs:
        op = op_
        vals.append(val)

    return AndOrChoice(op.capitalize(), vals)

def make_while(args):
    """Make a while loop into a ChoiceState

    Args
        args (tuple): (line:int, choice:Choice, (comment:string, steps:[State]))
                      choice: The loop conditional
                      steps: The loop body
    """
    line, choice, (comment, steps) = args
    name = make_name(line)

    choice['Next'] = str(steps[0])
    choices = ChoiceState(name, [choice])
    choices.line = line
    choices.branches = [steps]
    steps[-1]['Next'] = name # Create the loop
    add_name_comment(choices, comment)
    return choices

def make_if_else(args):
    """Make an if/elif/else into a ChoiceState

    Args
        args (tuple): (line:int, choice:Choice, (comment:string, steps:[State]), elif, else)
                      choice: The loop conditional
                      steps: The loop body
                      elif [(line:int, choice:Choice, (comment:string, steps:[State]))] The elif blocks (same as the if block)
                      else (line:int, (comment:string, steps:[State])) The else block
    """
    line, choice, (comment, steps), elif_, else_ = args

    branches = []
    choices = []

    choice['Next'] = str(steps[0])
    branches.append(steps)
    choices.append(choice)

    for line_, choice_, (_, steps_) in elif_:
        choice_['Next'] = str(steps_[0])
        branches.append(steps_)
        choices.append(choice_)

    if else_:
        line_, (_, steps_) = else_
        branches.append(steps_)
        default = str(steps_[0])
    else:
        default = None

    name = make_name(line)
    state = ChoiceState(name, choices, default)
    state.line = line
    state.branches = branches
    add_name_comment(state, comment)
    return state

def make_switch(args):
    """Make a switch statement into a ChoiceState

    Args
        args (tuple): (line:int,
                       var:string,
                       comment:string,
                       (val:int|float|string|ts, (comment:string, steps:[State])),
                       (comment:string, steps:[State]))

                      var: JsonPath of the variable to use in the case comparisons
                      val: Value to compare the variable against
                      steps: The case / default body
    """
    line, var, comment, cases, default = args

    choices = []
    branches = []

    for val, (_, steps_) in cases:
        choice = make_comp_simple((var, '==', val))
        choice['Next'] = str(steps_[0])
        branches.append(steps_)
        choices.append(choice)

    if default:
        (_, steps_) = default
        branches.append(steps_)
        default = str(steps_[0])

    name = make_name(line)
    state = ChoiceState(name, choices, default)
    state.line = line
    state.branches = branches
    add_name_comment(state, comment)
    return state

def make_parallel(args):
    """Make a ParallelState

    Args:
        args (tuple): (line:int, (comment:string, steps:[State]), parallels)
                      steps: The parallel body
                      parallels [(line:int, (comment:string, steps:[State]))] Additional parallel blocks
    """
    line, (comment, steps), parallels = args

    branches = []

    #DP ???: calling link in the middle of parsing. should this be called after all states are parsed??
    branches.append(Branch(link(steps), str(steps[0])))

    for line_, (_, steps_) in parallels:
        branches.append(Branch(link(steps_), str(steps_[0])))

    name = make_name(line)
    state = ParallelState(name, branches)
    state.line = line
    add_name_comment(state, comment)
    return state

def make_retry(args):
    """Make a Retry statement

    Args:
        args (tuple): (errors:string|list, interval:int, max:int, backoff:int|float)
                      errors: Single error or list of errors to catch
                              If an empty list, replaced with ['States.ALL']
                      interval: retry interval (seconds) for first retry
                      max: total number of retry attempts
                      backoff: multiplier applied to interval for each retry attempt
    """
    errors, interval, max_, backoff = args

    if errors == []:
        errors = ['States.ALL'] # match all errors if none is given
    return Retry(errors, interval, max_, backoff)

def make_catch(args):
    """Make a Catch statement

    Args:
        args (tuple): (errors:string|list, results:None|string, steps:[State])
                      errors: Single error or list of errors to catch
                              If an empty list, replaced with ['States.ALL']
                      results: ResultPath for where to place error information
                               in the input for the next state
                      steps: The catch body
    """
    errors, result_path, steps = args

    next_ = str(steps[0])

    if errors == []:
        errors = ['States.ALL'] # match all errors if none is given
    catch = Catch(errors, next_, result_path)
    catch.branches = [steps]
    return catch

def make_modifiers(args):
    """Take a mixed list of Catch and Retry statements and return a tuple of lists

    Args:
        args (list): List of Retry and Catch objects

    Returns:
        (retry, catch): retry is None or a list of Retry objects
                        catch is None or a list of Catch objects
    """
    retry = []
    catch = []
    for modifier in args:
        if type(modifier) == Retry:
            retry.append(modifier)
        elif type(modifier) == Catch:
            catch.append(modifier)
        else:
            raise Exception("Unknown modifier type: " + type(modifier).__name__)
    if retry == []:
        retry = None
    if catch == []:
        catch = None
    return (retry, catch)

def make_flow_modifiers(args):
    """Prepare flow control statements to be passed to add_modifiers

    Args:
        args (tuple): (state:State, transform:, errors:)
                      state: State to add the transform and error modifiers to
                      transform:
                      errors: An optional block
    Returns:
        tuple: Tuple formated for input to add_modifiers()
    """
    try:
        state, transform, errors = args
    except:
        state, transform = args
        errors = None

    return (state, (None, None, None, transform, None, errors))

def add_modifiers(args):
    """Add modifiers to a state

    Note all modifiers can be applied to all states. If a modifier is given
    that cannot be applied to the given state, and exception is raised.

    Args:
        args (tuple): (state:State,
                       (
                            comment:string,
                            timeout:int
                            heartbeat:int,
                            (
                                input:string,
                                 result:string,
                                 output:string
                            ),
                            data:dict,
                            (
                                retries:[Retry],
                                catches:[Catch]
                            )
                       )
                      )
                      timeout: task timeout
                      heartbeat: task heartbeat
                      input: input JsonPath selector
                      result: result JsonPath selector
                      output: output JsonPath selector
                      data: PassState Json results
                      retries: list of Retries
                      catches: list of Catches
    """
    state, args = args

    type_ = type(state)

    # DP TODO: Create AST so that CompileError context can be filled in
    error = lambda m: CompileError(state.line, 0, '', m)

    if args:
        comment, timeout, heartbeat, transform, data, modifiers = args

        add_name_comment(state, comment)

        if timeout:
            if type_ not in (TaskState,):
                raise error("Invalid modifier 'timeout'")
            state['TmeoutSeconds'] = timeout
        else:
            timeout = 60

        if heartbeat:
            if type_ not in (TaskState,):
                raise error("Invalid modifier 'heartbeat'")
            if not heartbeat < timeout:
                raise error("Modifier 'heartbeat' must be less than 'timeout'")
            state['HeartbeatSeconds'] = heartbeat

        if transform:
            input_path, result_path, output_path = transform

            if input_path:
                if type_ in (FailState,):
                    raise error("Invalid modifier 'input'")
                state['InputPath'] = input_path

            if result_path:
                if type_ in (FailState, SuccessState, WaitState):
                    raise error("Invalid modifier 'result'")
                state['ResultPath'] = result_path

            if output_path:
                if type_ in (FailState,):
                    raise error("Invalid modifier 'output'")
                state['OutputPath'] = output_path

        if data:
            if type_ != PassState:
                raise error("Invalid modifier 'data'")
            state['Result'] = data

        if modifiers:
            retries, catches = modifiers
            if retries:
                if type_ not in (TaskState, ParallelState):
                    raise error("Invalid modifier 'retry'")
                state['Retry'] = retries
            if catches:
                if type_ not in (TaskState, ParallelState):
                    raise error("Invalid modifier 'catch'")
                state['Catch'] = catches
                state.branches = []
                for catch in catches:
                    state.branches.extend(catch.branches)

    return state

def json_text():
    """Returns the parser for Json formatted data"""
    # Taken from https://github.com/vlasovskikh/funcparserlib/blob/master/funcparserlib/tests/json.py
    # and modified slightly
    null = (n('null') | n('Null')) >> const(None)
    true = (n('true') | n('True')) >> const(True)
    false = (n('false') | n('False')) >> const(False)
    number = toktype('NUMBER') >> make_number
    string = toktype('STRING') >> make_string

    value = forward_decl()
    member = string + op_(u':') + value >> tuple
    object = (
        op_(u'{') +
        maybe(member + many(op_(u',') + member) + maybe(op_(','))) +
        op_(u'}')
        >> make_object)
    array = (
        op_(u'[') +
        maybe(value + many(op_(u',') + value) + maybe(op_(','))) +
        op_(u']')
        >> make_array)

    value.define(
        null
        | true
        | false
        | object
        | array
        | number
        | string)
    json_text = object | array

    return json_text

def comparison():
    ops = op('==') | op('<') | op('>') | op('<=') | op('>=') | op('!=')
    op_vals = (boolean|number|string|timestamp)
    comp_op = string + ops + op_vals >> ASTCompOp

    comp_stmt = forward_decl()
    comp_base = forward_decl()
    comp_base.define((op_('(') + comp_stmt + op_(')')) | comp_op | ((n('not') + comp_base) >> ASTCompNot))
    comp_and = comp_base + many(n('and') + comp_base) >>  ASTCompAnd
    comp_or = comp_and + many(n('or') + comp_and) >> ASTCompOr
    comp_stmt.define(comp_or)

    return comp_stmt

def parse(seq, region=None, account=None, translate=lambda x: x):
    """Parse the given sequence of tokens into a StateMachine object

    Args:
        seq (list): List of lexer.Token tokens to parse
        region (string): AWS region for constructed Lambda/Activity ARNs
        account (string): AWS account id for constructed Lambda/Activity ARNs
        translate (function): Translation function applied to Lambda/Activity names
                              before ARNs are constructed

    Returns
        sfn.StateMachine: StateMachine object
    """
    def make_task(args):
        """Make a TaskState

        Args:
            args (tuple): ((line number:int, type:string), function:string)
                          type: 'Lambda' or 'Activity'
                          function: Full or partial ARN of the Lambda or Activity
        """
        (line, type_), func = args

        func = translate(func)

        name = make_name(line)
        try:
            if type_ == "Lambda":
                task = Lambda(func, region, account)
            elif type_ == "Activity":
                task = Activity(func, region, account)
            else:
                raise Exception("Unsuported task type '{}'".format(type_))
        except Exception as e:
            raise CompileError(line, 0, '', str(e))

        state = TaskState(name, task)
        state.line = line
        return state

    state = forward_decl()

    # Primatives
    array = op_('[') + maybe(string + many(op_(',') + string)) + op_(']') >> make_array

    block = block_s + many(state) + block_e
    retry_block = n('retry') + (array|string) + number + number + number >> make(ASTModRetry)
    catch_block = n('catch') + (array|string) + op_(':') + maybe(string) + block >> make(ASTModCatch)


    # Simple States
    state_modifier = ((n('timeout') + op_(':') + number >> make(ASTModTimeout)) |
                      (n('heartbeat') + op_(':') + number >> make(ASTModHeartbeat)) |
                      (n('input') + op_(':') + string >> make(ASTModInput)) |
                      (n('result') + op_(':') + string >> make(ASTModResult)) |
                      (n('output') + op_(':') + string >> make(ASTModOutput)) |
                      (n('data') + op_(':') + json_text() >> make(ASTModData)) |
                      retry_block | catch_block)

    state_modifiers = state_modifier + many(state_modifier) >> make(ASTModifiers)
    state_block = maybe(block_s + maybe(string) + maybe(state_modifiers) + block_e)

    pass_ = n('Pass') + op_('(') + op_(')') + state_block >> make(ASTStatePass)
    success = n('Success') + op_('(') + op_(')') + state_block >> make(ASTStateSuccess)
    fail = n('Fail') + op_('(') + string + op_(',') + string + op_(')') + state_block >> make(ASTStateFail)
    task = (n('Lambda') | n('Activity')) + op_('(') + string + op_(')') + state_block >> make(ASTStateTask)
    wait_types = n('seconds') | n('seconds_path') | n('timestamp') | n('timestamp_path')
    wait = l('Wait') + op_('(') + wait_types + op_('=') + (number|string|timestamp) + op_(')') + state_block >> make(ASTStateWait)
    simple_state = pass_ | success | fail | task | wait

    """
    # Flow Control blocks
    transform_block = maybe(n_('transform') + op_(':') + block_s + maybe(mod_transform) + block_e)
    error_block = maybe(n_('error') + op_(':') + block_s + maybe(mod_error) + block_e)
    block = block_s + maybe(string) + many(state) + block_e

    # Control Flow states
    comparison = comp_stmt + op_(':')
    while_ = l('while') + comparison + block >> make_while
    if_else = (l('if') + comparison + block +
               many(l('elif') + comparison + block) +
               maybe(l('else') + op_(':') + block)) >> make_if_else
    case = n_('case') + (boolean|number|ts_str) + op_(':') + block
    switch = (l('switch') + string + op_(':') +
              block_s + maybe(string) + many(case) +
              maybe(n_('default') + op_(':') + block) +
              block_e) >> make_switch
    choice_state = while_ | if_else | switch
    choice_state_ = (choice_state + transform_block) >> make_flow_modifiers >> add_modifiers
    parallel = (l('parallel') + op_(':') + block +
                many(l('parallel') + op_(':') + block)) >> make_parallel
    parallel_ = (parallel + transform_block + error_block) >> make_flow_modifiers >> add_modifiers
    #state.define(simple_state_ | choice_state_ | parallel_)
    """
    state.define(simple_state)

    #import code
    #code.interact(local=locals())

    # State Machine
    version = maybe(n('version') + op_(':') + string >> make(ASTModVersion))
    timeout = maybe(n('timeout') + op_(':') + number >> make(ASTModTimeout))
    machine = maybe(string) + version + timeout + many(state) + end >> make(ASTStepFunction)

    try:
        # call run() directly to have better control of error handling
        (tree, _) = machine.run(seq, State())
        link(tree)
        function = StepFunction(tree)
        # TODO Link / modify the AST to be in the format needed to convert to JSON
        return function
    except NoParseError as e:
        max = e.state.max
        tok = seq[max] if len(seq) > max else Token('EOF', '<EOF>')

        if tok.code == 'ERRORTOKEN':
            msg = "Unterminated quote"
        else:
            msg = "Invalid syntax"
            # DP ???: Should the actual token be used in the error message?

        raise CompileError.from_token(tok, msg)

