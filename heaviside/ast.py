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

from funcparserlib.parser import (some, a, skip)

from .lexer import Token
from .exceptions import CompileError

# AST Objects
class ASTNode(object):
    def __init__(self, token=None):
        if token is not None:
            self.token = token
        else:
            self.token = None

    @property
    def lineno(self):
        return self.token.start[0] if self.token else 0

    @property
    def pos(self):
        return self.token.start[1] if self.token else 0

    @property
    def line(self):
        return self.token.line if self.token else ''

    def raise_error(self, msg):
        raise CompileError(self.lineno,
                           self.pos,
                           self.line,
                           msg)

class ASTValue(ASTNode):
    def __init__(self, value, token):
        super(ASTValue, self).__init__(token)
        self.value = value

class ASTCompOp(ASTNode):
    def __init__(self, var, op, val):
        # Use the first token of the expression
        super(ASTCompOp, self).__init__(var.token)
        self.var = var
        self.op = op
        self.val = val

class ASTCompNot(ASTNode):
    def __init__(self, not_, comp):
        super(ASTCompNot, self).__init__(not_.token)
        self.comp = comp

class ASTCompAndOr(ASTNode):
    def __init__(self, comp, comps):
        super(ASTCompAndOr, self).__init__(comp.token)
        self.comps = [comp]
        for c in comps:
            self.comps.append(comp)

class ASTCompAnd(ASTCompAndOr):
    pass

class ASTCompOr(ASTCompAndOr):
    pass

class ASTModKV(ASTValue):
    def __init__(self, key, value):
        super(ASTModKV, self).__init__(value, key.token)

class ASTModTimeout(ASTModKV):
    pass

class ASTModHeartbeat(ASTModKV):
    pass

class ASTModInput(ASTModKV):
    pass

class ASTModResult(ASTModKV):
    pass

class ASTModOutput(ASTModKV):
    pass

class ASTModData(ASTModKV):
    pass

class ASTModRetry(ASTNode):
    def __init__(self, retry, errors, interval, max, backoff):
        super(ASTModRetry, self).__init__(retry.token)
        self.errors = errors
        self.interval = interval
        self.max = max
        self.backoff = backoff

class ASTModCatch(ASTNode):
    def __init__(self, catch, errors, path, block):
        super(ASTModCatch, self).__init__(catch.token)
        self.errors = errors
        self.path = path
        self.block = block

class ASTModifiers(ASTNode): #??? Subclass dict as well?
    def __init__(self, mod, mods):
        super(ASTModifiers, self).__init__(mod.token)
        self.mods = {}

        self.add(mod)
        for m in mods:
            self.add(m)

    def add(self, mod):
        t = type(mod)
        if t not in self.mods:
            self.mods[t] = []
        self.mods[t].append(mod)

class ASTState(ASTNode):
    state_type = ''
    valid_modifiers = []
    multi_modifiers = [ASTModRetry, ASTModCatch]

    def __init__(self, state, block):
        super(ASTState, self).__init__(state.token)
        if block:
            comment, modifiers = block
        else:
            comment = None
            modifiers = None

        if comment:
            name, comment = comment.split('\n', 1)
            self.name = name.strip()
            self.comment = '\n'.join([l.strip() for l in comment.split('\n')])
        else:
            self.name = 'Line{}'.format(self.lineno)
            self.comment = None

        def get(type_):
            if modifiers is None:
                return None

            vals = modifiers.mods.get(type_)
            
            if vals is None:
                return None

            del modifiers.mods[type_]

            if type_ not in self.valid_modifiers:
                vals[0].raise_error("Not a valid modifier for a {} state".format(self.state_type))

            if type_ not in self.multi_modifiers:
                if len(vals) > 1:
                    vals[1].raise_error("Can only specify modifier once per state")

                vals = vals[0]

            return vals

        self.timeout = get(ASTModTimeout)
        self.heartbeat = get(ASTModHeartbeat)
        self.input = get(ASTModInput)
        self.result = get(ASTModResult)
        self.output = get(ASTModOutput)
        self.data = get(ASTModData)
        self.retry = get(ASTModRetry)
        self.catch = get(ASTModCatch)

        if modifiers is not None and len(modifiers.mods) > 0:
            for type_ in modifiers.mods:
                modifiers.mods[type_][0].raise_error("Unknown state modifer '{}'".format(type_))

class ASTStatePass(ASTState):
    state_type = 'Pass'
    valid_modifiers = [ASTModInput, ASTModResult, ASTModOutput, ASTModData]

class ASTStateSuccess(ASTState):
    state_type = 'Succeed'
    valid_modifiers = [ASTModInput, ASTModOutput]

class ASTStateFail(ASTState):
    state_type = 'Fail'

    def __init__(self, state, error, cause, block):
        super(ASTStateFail, self).__init__(state, block)
        self.error = error
        self.cause = cause

class ASTStateTask(ASTState):
    state_type = 'Task'
    valid_modifiers = []

    def __init__(self, state, arn, block):
        super(ASTStateTask, self).__init__(state, block)
        self.arn = arn

class ASTStateWait(ASTState):
    state_Type = 'Wait'
    valid_modifiers = []

    def __init__(self, state, wait_type, wait_val, block):
        super(ASTStateWait, self).__init__(state, block)
        self.type = wait_type
        self.val = wait_val

class ASTModVersion(ASTModKV):
    pass

class ASTStepFunction(ASTNode):
    def __init__(self, comment, version, timeout, states):
        super(ASTStepFunction, self).__init__() # ???: use the first states's token?
        self.comment = comment
        self.version = version
        self.timeout = timeout
        self.states = states

def link(ast, final=None):
    linked = []

    if type(ast) == ASTStepFunction:
        states = ast.states
    else:
        return ast

    total = len(states)
    for i in range(total):
        state = states[i]

        linked.append(state)

        next_ = states[i+1].name if i+1 < total else final
        if hasattr(state, 'next') or hasattr(state, 'end'):
            pass # State has already been linked
        elif type(state) in (ASTStateSuccess, ASTStateFail):
            pass # Terminal States
        else:
            state.end = next_ is None
            state.next = next_

    ast.states = linked

def check_names(states):
    names = [s.name for s in states]
    # find non unique names in names list

