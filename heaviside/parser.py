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

def make(cls):
    def make_(args):
        return cls(*args)
    return make_

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

def const(value):
    def const_(token):
        return ASTValue(value, token)
    return const_

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

# DP ???: toktype('STRING') instead of some(lambda x: ...) ??
string = some(lambda x: x.code =='STRING') >> tok_to_value >> value_to_string

def string_to_timestamp(ast):
    try:
        ast.value = Timestamp(ast.value)
    except:
        ast.raise_error("'{}' is not a valid timestamp".format(ast.value))
    return ast

# NOTE: If parsing for both timestamp or string, place timestamp first
#       Because a timestamp is a string, if string is placed first, the
#       expression will always return a string value
timestamp = string >> string_to_timestamp

end = skip(a(Token('ENDMARKER', '')))
block_s = skip(toktype('INDENT'))
block_e = skip(toktype('DEDENT'))

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

#=============
# Parser Rules
#=============
def json_text():
    """Returns the parser for Json formatted data"""
    # Taken from https://github.com/vlasovskikh/funcparserlib/blob/master/funcparserlib/tests/json.py
    # and modified slightly
    null = (n('null') | n('Null')) >> const(None)

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

def comparison_():
    ops = op('==') | op('<') | op('>') | op('<=') | op('>=') | op('!=')
    op_vals = (boolean|number|string|timestamp)
    comp_op = string + ops + op_vals >> make(ASTCompOp)

    def multi(func):
        """For x + many(x) lists, call func only when there are multiple xs"""
        def multi_(args):
            x, xs = args
            if len(xs) == 0:
                return x
            return func(args)
        return multi_

    comp_stmt = forward_decl()
    comp_base = forward_decl()
    comp_base.define((op_('(') + comp_stmt + op_(')')) | comp_op | ((n('not') + comp_base) >> make(ASTCompNot)))
    comp_and = comp_base + many(n('and') + comp_base) >>  multi(make(ASTCompAnd))
    comp_or = comp_and + many(n('or') + comp_and) >> multi(make(ASTCompOr))
    comp_stmt.define(comp_or)

    return comp_stmt
comparison = comparison_()

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
    state = forward_decl()

    # Primatives
    array = op_('[') + maybe(string + many(op_(',') + string)) + op_(']') >> make_array

    block = block_s + many(state) + block_e
    comment_block = block_s + maybe(string) + many(state) + block_e
    retry_block = n('retry') + (array|string) + number + number + number >> make(ASTModRetry)
    catch_block = n('catch') + (array|string) + op_(':') + maybe(string) + block >> make(ASTModCatch)


    # Simple States
    state_modifier = ((n('timeout') + op_(':') + number >> make(ASTModTimeout)) |
                      (n('heartbeat') + op_(':') + number >> make(ASTModHeartbeat)) |
                      (n('input') + op_(':') + string >> make(ASTModInput)) |
                      (n('result') + op_(':') + string >> make(ASTModResult)) |
                      (n('output') + op_(':') + string >> make(ASTModOutput)) |
                      (n('data') + op_(':') + block_s + json_text() + block_e >> make(ASTModData)) |
                      retry_block | catch_block)

    state_modifiers = state_modifier + many(state_modifier) >> make(ASTModifiers)
    state_block = maybe(block_s + maybe(string) + maybe(state_modifiers) + block_e)

    pass_ = n('Pass') + op_('(') + op_(')') + state_block >> make(ASTStatePass)
    success = n('Success') + op_('(') + op_(')') + state_block >> make(ASTStateSuccess)
    fail = n('Fail') + op_('(') + string + op_(',') + string + op_(')') + state_block >> make(ASTStateFail)
    task = (n('Lambda') | n('Activity')) + op_('(') + string + op_(')') + state_block >> make(ASTStateTask)
    wait_types = n('seconds') | n('seconds_path') | n('timestamp') | n('timestamp_path')
    wait = n('Wait') + op_('(') + wait_types + op_('=') + (number|string|timestamp|string) + op_(')') + state_block >> make(ASTStateWait)
    simple_state = pass_ | success | fail | task | wait

    # Flow Control States
    transform_modifier = ((n('input') + op_(':') + string >> make(ASTModInput)) |
                          (n('result') + op_(':') + string >> make(ASTModResult)) |
                          (n('output') + op_(':') + string >> make(ASTModOutput)))
    transform_modifiers = transform_modifier + many(transform_modifier) >> make(ASTModifiers)
    transform_block = maybe(n_('transform') + op_(':') + block_s + maybe(transform_modifiers) + block_e)

    while_ = n('while') + comparison + op_(':') + comment_block + transform_block >> make(ASTStateWhile)
    if_else = (n('if') + comparison + op_(':') + comment_block +
               many(n_('elif') + comparison + op_(':') + block) +
               maybe(n_('else') + op_(':') + block) + transform_block) >> make(ASTStateIfElse)
    switch_case = n('case') + (boolean|number|timestamp|string) + op_(':') + block
    switch = (n('switch') + string + op_(':') +
              block_s + maybe(string) + many(switch_case) +
              maybe(n('default') + op_(':') + block) +
              block_e + transform_block) >> make(ASTStateSwitch)
    choice_state = while_ | if_else | switch

    error_modifier = (retry_block|catch_block) + many(retry_block|catch_block) >> make(ASTModifiers)
    error_block = maybe(n_('error') + op_(':') + block_s + maybe(error_modifier) + block_e)
    parallel = (n('parallel') + op_(':') + comment_block +
                many(n('parallel') + op_(':') + block) +
                transform_block + error_block) >> make(ASTStateParallel)

    state.define(simple_state | choice_state | parallel)

    # State Machine
    version = maybe(n('version') + op_(':') + string >> make(ASTModVersion))
    timeout = maybe(n('timeout') + op_(':') + number >> make(ASTModTimeout))
    machine = maybe(string) + version + timeout + many(state) + end >> make(ASTStepFunction)

    try:
        # DP NOTE: call run() directly to have better control of error handling
        (tree, _) = machine.run(seq, State())
        link_branch(tree)
        # TODO: check state names (As seperate step or part of link)
        # TODO: callback to resolve lambda and activity arns
        function = StepFunction(tree)
        #import code
        #code.interact(local=locals())

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

