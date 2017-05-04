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

from .sfn import StepFunction, Timestamp

def make(cls):
    """Helper that unpacks the tuple of arguments before creating a class"""
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

number = toktype('NUMBER') >> value_to_number

def check(cond, msg):
    def check_(ast):
        if not cond(ast.value):
            ast.raise_error(msg.format(ast.value))
        return ast
    return check_

integer = number >> check(lambda val: isinstance(val, int), "'{}' is not a valid integer")
integer_nn = integer >> check(lambda val: val >= 0, "'{}' is not a non-negative integer")
integer_pos = integer >> check(lambda val: val > 0, "'{}' is not a positive integer")

def value_to_string(ast):
    if ast.value[:3] in ('"""', "'''"):
        ast.value = ast.value[3:-3]
    else:
        ast.value = ast.value[1:-1]
    return ast

string = toktype('STRING') >> value_to_string

def string_to_timestamp(ast):
    try:
        ast.value = Timestamp(ast.value)
    except:
        pass
        #ast.raise_error("'{}' is not a valid timestamp".format(ast.value))
    return ast

timestamp_or_string = string >> string_to_timestamp

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
    unwrap = lambda x: x.value

    null = (n('null') | n('Null')) >> const(None) >> unwrap

    value = forward_decl()
    member = (string >> unwrap) + op_(u':') + value >> tuple
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
        | (true >> unwrap)
        | (false >> unwrap)
        | object
        | array
        | (number >> unwrap)
        | (string >> unwrap))
    json_text = object | array

    return json_text

def comparison_():
    """Returns the parse for a compound compare statement"""
    ops = op('==') | op('<') | op('>') | op('<=') | op('>=') | op('!=')
    op_vals = (boolean|number|timestamp_or_string)
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
    comp_and = comp_base + many(n_('and') + comp_base) >> multi(make(ASTCompAnd))
    comp_or = comp_and + many(n_('or') + comp_and) >> multi(make(ASTCompOr))
    comp_stmt.define(comp_or)

    return comp_stmt
comparison = comparison_()

def parse(seq, translate=lambda x, y: y):
    """Parse the given sequence of tokens into a StateMachine object

    Args:
        seq (list): List of lexer.Token tokens to parse
        translate (function): Translation function applied to Lambda/Activity names.
                              Arguments are ("Lambda"|"Activity", arn)

    Returns
        sfn.StateMachine: StateMachine object
    """
    state = forward_decl()

    # Primatives
    array = op_('[') + maybe(string + many(op_(',') + string)) + op_(']') >> make_array

    block = block_s + many(state) + block_e
    comment_block = block_s + maybe(string) + many(state) + block_e
    retry_block = n('retry') + (array|string) + integer_pos + integer_nn + number >> make(ASTModRetry)
    catch_block = n('catch') + (array|string) + op_(':') + maybe(string) + block >> make(ASTModCatch)


    # Simple States
    state_modifier = ((n('timeout') + op_(':') + integer_pos >> make(ASTModTimeout)) |
                      (n('heartbeat') + op_(':') + integer_pos >> make(ASTModHeartbeat)) |
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
    wait = n('Wait') + op_('(') + wait_types + op_('=') + (integer_pos|timestamp_or_string) + op_(')') + state_block >> make(ASTStateWait)
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
    switch_case = n('case') + (boolean|number|timestamp_or_string) + op_(':') + block
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
    timeout = maybe(n('timeout') + op_(':') + integer_pos >> make(ASTModTimeout))
    machine = maybe(string) + version + timeout + many(state) + end >> make(ASTStepFunction)

    try:
        # DP NOTE: calling run() directly to have better control of error handling
        (tree, _) = machine.run(seq, State())
        link_branch(tree)
        check_names(tree)
        resolve_arns(tree, translate)
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

