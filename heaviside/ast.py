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

from collections import OrderedDict

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

    def __repr__(self):
        return "ASTValue({!r})".format(self.value)

class ASTCompOp(ASTNode):
    def __init__(self, var, op, val):
        # Use the first token of the expression
        super(ASTCompOp, self).__init__(var.token)
        self.var = var
        self.op = op
        self.val = val

    def __repr__(self):
        return "ASTCompOp({!r} {!r} {!r})".format(self.var, self.op, self.val)

class ASTCompNot(ASTNode):
    def __init__(self, not_, comp):
        super(ASTCompNot, self).__init__(not_.token)
        self.comp = comp

    def __repr__(self):
        return "ASTCompNot({!r})".format(self.comp)

class ASTCompAndOr(ASTNode):
    op = None
    def __init__(self, comp, comps):
        super(ASTCompAndOr, self).__init__(comp.token)
        self.comps = [comp]
        for c in comps:
            self.comps.append(c)

    def __repr__(self):
        return "ASTComp{}({!r})".format(self.op, self.comps)

class ASTCompAnd(ASTCompAndOr):
    op = 'And'

class ASTCompOr(ASTCompAndOr):
    op = 'Or'

class ASTModKV(ASTValue):
    def __init__(self, key, value):
        super(ASTModKV, self).__init__(value, key.token)

class ASTModTimeout(ASTModKV):
    name = 'Timeout'

class ASTModHeartbeat(ASTModKV):
    name = 'Heartbeat'

class ASTModInput(ASTModKV):
    name = 'Input'

class ASTModResult(ASTModKV):
    name = 'Result'

class ASTModOutput(ASTModKV):
    name = 'Output'

class ASTModData(ASTModKV):
    name = 'Data'

class ASTModRetry(ASTNode):
    name = 'Retry'

    def __init__(self, retry, errors, interval, max, backoff):
        super(ASTModRetry, self).__init__(retry.token)
        self.errors = errors
        self.interval = interval
        self.max = max
        self.backoff = backoff

class ASTModCatch(ASTNode):
    name = 'Catch'

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

    def update(self, other):
        for key in other.mods.keys():
            if key not in self.mods:
                self.mods[key] = []
            self.mods[key].extend(other.mods[key])

class ASTState(ASTNode):
    state_type = ''
    valid_modifiers = []
    multi_modifiers = [ASTModRetry, ASTModCatch]

    def __init__(self, state, block):
        super(ASTState, self).__init__(state.token)
        self.next = None
        self.end = False

        if block:
            comment, modifiers = block
        else:
            comment = None
            modifiers = None

        if comment:
            tmp = comment.value.split('\n', 1)
            if len(tmp) == 1:
                self.name = tmp[0].strip()
                self.comment = None
            else:
                name, comment = tmp
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

            name = type_.name if hasattr(type_, 'name') else str(type_.__name__)

            if type_ not in self.valid_modifiers:
                vals[0].raise_error("{} state cannot contain a {} modifier".format(self.state_type, name))

            if type_ not in self.multi_modifiers:
                if len(vals) > 1:
                    vals[1].raise_error("{} state can only contain one {} modifier".format(self.state_type, name))

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
            type_ = list(modifiers.mods.keys())[0]
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
    valid_modifiers = [ASTModTimeout,
                       ASTModHeartbeat,
                       ASTModInput,
                       ASTModResult,
                       ASTModOutput,
                       ASTModRetry,
                       ASTModCatch]

    def __init__(self, state, arn, block):
        super(ASTStateTask, self).__init__(state, block)
        self.arn = arn

class ASTStateWait(ASTState):
    state_type = 'Wait'
    valid_modifiers = [ASTModInput, ASTModOutput]

    def __init__(self, state, wait_type, wait_val, block):
        super(ASTStateWait, self).__init__(state, block)
        self.type = wait_type
        self.val = wait_val

class ASTStateChoice(ASTState):
    state_type = 'Choice'
    valid_modifiers = [ASTModInput, ASTModOutput]
    DEFAULT = None

    def __init__(self, state, comment, transform):
        super(ASTStateChoice, self).__init__(state, (comment, transform))

        # Use an OrderedDict so that logic comparisons happen in the
        # same order as in the source file
        self.branches = OrderedDict()

# DP ???: Should ASTStateChoice subclasses override the state_type value?
class ASTStateWhile(ASTStateChoice):
    def __init__(self, state, comp, block, transform):
        comment, states = block
        super(ASTStateWhile, self).__init__(state, comment, transform)

        self.branches[comp] = states

class ASTStateIfElse(ASTStateChoice):
    def __init__(self, state, comp, block, elif_, else_, transform):
        comment, states = block
        super(ASTStateIfElse, self).__init__(state, comment, transform)

        self.branches[comp] = states

        if elif_ is not None:
            for comp_, states_ in elif_:
                self.branches[comp_] = states_

        if else_ is not None:
            self.branches[ASTStateChoice.DEFAULT] = else_

class ASTStateSwitch(ASTStateChoice):
    def __init__(self, state, var, comment, cases, default, transform):
        super(ASTStateSwitch, self).__init__(state, comment, transform)

        class EQToken(object):
            def __init__(self):
                self.value = '=='
        eq = EQToken()

        for case, val, states in cases:
            comp = ASTCompOp(var, eq, val)
            self.branches[comp] = states

        if default is not None:
            default, states = default
            self.branches[ASTStateChoice.DEFAULT] = states

class ASTParallelBranch(ASTNode):
    def __init__(self, parallel, states):
        super(ASTParallelBranch, self).__init__(parallel.token)
        self.states = states

class ASTStateParallel(ASTState):
    state_type = 'Parallel'
    valid_modifiers = [ASTModInput,
                       ASTModResult,
                       ASTModOutput,
                       ASTModRetry,
                       ASTModCatch]

    def __init__(self, state, block, parallels, transform, error):
        comment, states = block
        if transform is not None and error is not None:
            transform.update(error)
        elif transform is None and error is not None:
            transform = error
        super(ASTStateParallel, self).__init__(state, (comment, transform))

        self.branches = [ASTParallelBranch(state, states)]
        if parallels is not None:
            for parallel, states_ in parallels:
                self.branches.append(ASTParallelBranch(parallel, states_))

class ASTModVersion(ASTModKV):
    pass

class ASTStepFunction(ASTNode):
    def __init__(self, comment, version, timeout, states):
        super(ASTStepFunction, self).__init__() # ???: use the first states's token?
        self.comment = comment
        self.version = version
        self.timeout = timeout
        self.states = states

##############################
# AST Modification Functions #

TERMINAL_STATES = (
    ASTStateSuccess,
    ASTStateFail
)

def link_branch(branch):
    """Helper method for link() that reassigns the results to the given branch"""
    if hasattr(branch, 'states'):
        branch.states = link(branch.states)
    else:
        branch.raise_error("Trying to link non-branch state")
    return branch

def link(states, final=None):
    """AST Transform function that links together the states of a branch

    Performs the following actions:
        * Sets the next / end attributes for all encountered ASTState objects
        * If the final state is a ASTStateChoice and there is no default state
          one is created, as you cannot terminate on a Choice state
        * Makes the ASTStateWhile into a full loop
        * If there is a Catch modifier or the state is a Choice state the sub-states
          for each are recursivly linked and pulled up to the current level
        * The branches for all Parallel states are recursivly linked

    Args:
        states (list) : List of ASTState objects
        final (String) : Name of the next state to link the final state to
                         or None to have the final state terminate

    Returns:
        list : List of ASTState objects with end/next set
               Note: This is a different list than the input list
    """
    linked = []

    total = len(states)
    for i in range(total):
        state = states[i]

        linked.append(state)

        next_ = states[i+1].name if i+1 < total else final
        if state.next is not None:
            pass # State has already been linked
        elif isinstance(state, TERMINAL_STATES):
            pass
        elif isinstance(state, ASTStateChoice):
            if ASTStateChoice.DEFAULT not in state.branches:
                next__ = next_ # prevent branches from using the new end state
                if next__ is None:
                    # Choice cannot be terminal state, add a Success state to
                    # terminate on
                    next__ = ASTStateSuccess(state, None)
                    next__.name = state.name + "Default"
                    linked.append(next__)
                    next__ = next__.name
                state.branches[ASTStateChoice.DEFAULT] = next__

            # Point the last state of the loop to the conditional, completing the loop construct
            if isinstance(state, ASTStateWhile):
                key = list(state.branches.keys())[0]
                state_ = state.branches[key][-1]
                if not isinstance(state_, TERMINAL_STATES):
                    state_.next = state.name

        else:
            state.end = next_ is None
            state.next = next_

        if state.catch is not None:
            for catch in state.catch:
                states_ = catch.block
                linked_ = link(states_, final=next_)
                catch.next = linked_[0].name
                linked.extend(linked_)

        # Different states use the branches variable in different ways
        if isinstance(state, ASTStateChoice):
            for key in state.branches:
                states_ = state.branches[key]
                if isinstance(states_, str):
                    continue # already linked
                linked_ = link(state.branches[key], final=next_)
                # convert the branch from a list of states to the name of the next state
                # this is done because the branch states are moved to the appropriate
                # location for the step function
                state.branches[key] = linked_[0].name
                linked.extend(linked_)
        elif isinstance(state, ASTStateParallel):
            for branch in state.branches:
                link_branch(branch)

    return linked

MAX_NAME_LENGTH = 128
def check_names(branch):
    """Recursivly checks all state names to ensure they are valid

    Checks performed:
        * Name is not greater than 128 characters
        * No duplicate state names
    """
    if not hasattr(branch, 'states'):
        branch.raise_error("Trying to check names for non-branch state")

    to_process = [branch.states]
    names = set()

    while len(to_process) > 0:
        states = to_process.pop(0)

        for state in states:
            if len(state.name) > MAX_NAME_LENGTH:
                state.raise_error("Name exceedes {} characters".format(MAX_NAME_LENGTH))

            if state.name in names:
                state.raise_error("Duplicate state name '{}'".format(state.name))

            names.add(state.name)

            if isinstance(state, ASTStateParallel):
                for branch in state.branches:
                    to_process.append(branch.states)

def resolve_arns(branch, translate = lambda x, y: y):
    """AST Transform that looks for all Task states and passed the task type
    and (partial) ARN to a callback. Used for resolving partial ARNs to full
    ARNs.

    Args:
        branch (list): List of ASTState objects
        translate (callable): Callable that receives task type and ARN
                              Returned ARN replaces the current value in the
                              ASTStateTask
    """
    if not hasattr(branch, 'states'):
        branch.raise_error("Trying to resolve arns for non-branch state")

    for state in branch.states:
        if isinstance(state, ASTStateTask):
            try:
                state.arn.value = translate(state.token.value, state.arn.value)
            except Exception as e:
                state.raise_error(str(e))

        elif isinstance(state, ASTStateParallel):
            for branch in state.branches:
                resolve_arns(branch, translate)

