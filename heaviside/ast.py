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

import copy
from collections import OrderedDict
from .exceptions import CompileError
from .utils import isstr

#################################################
#### Load AWS Service Integration Definitions####
#################################################

import json

try:
    from importlib.resources import read_text
except ImportError:
    from importlib_resources import read_text

AWS_SERVICES = json.loads(read_text('heaviside', 'aws_services.json'))

#################################################
#################################################

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

    def __repr__(self):
        return "<ASTModKV {}:{}>".format(self.name, self.value)

class ASTModNext(ASTModKV):
    name = 'Next'

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

class ASTModParameters(OrderedDict, ASTNode):
    name = 'Parameters'

    # NOTE: kpv stands for (key, path marker, value)
    #       where `path marker` is the token for the `$` that denotes is the
    #       value contains a JsonPath
    def __init__(self, parameters, kpv, kpvs):
        OrderedDict.__init__(self)
        ASTNode.__init__(self, parameters.token)

        self.add_parameter(kpv)
        for kpv in kpvs:
            self.add_parameter(kpv)

    def add_parameter(self, kpv):
        k,p,v = kpv
        if p is not None:
            k.value += '.$' # Parameters that use a JsonPath must have the key
                            # end with `.$`

        self[k] = v

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

    def __repr__(self):
        return "\n".join(repr(mod) for mod in self.mods.values())

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

        self.next = get(ASTModNext)
        self.timeout = get(ASTModTimeout)
        self.heartbeat = get(ASTModHeartbeat)
        self.input = get(ASTModInput)
        self.result = get(ASTModResult)
        self.output = get(ASTModOutput)
        self.data = get(ASTModData)
        self.parameters = get(ASTModParameters)
        self.retry = get(ASTModRetry)
        self.catch = get(ASTModCatch)
        self.iterator = get(ASTModIterator)
        self.max_concurrency = get(ASTModMaxConcurrency)
        self.items_path = get(ASTModItemsPath)

        if modifiers is not None and len(modifiers.mods) > 0:
            type_ = list(modifiers.mods.keys())[0]
            modifiers.mods[type_][0].raise_error("Unknown state modifer '{}'".format(type_))

    def __repr__(self):
        return "<ASTState {}:{}>".format(self.state_type, self.name)

class ASTStatePass(ASTState):
    state_type = 'Pass'
    valid_modifiers = [ASTModInput, ASTModResult, ASTModOutput, ASTModData, ASTModParameters]

class ASTStateGoto(ASTStatePass):
    """Custom version of Pass that exposes / sets the 'next' modifier"""
    valid_modifiers = [ASTModNext]

    def __init__(self, state, label):
        # Create the state_modifer block manually
        block = (None,
                 ASTModifiers(
                    ASTModNext(label, label.value),
                    []
                 )
                )

        super(ASTStateGoto, self).__init__(state, block)

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
                       ASTModParameters,
                       ASTModRetry,
                       ASTModCatch]

    valid_services = ['Arn',
                      'Lambda',
                      'Activity']

    def __init__(self, service, function, name, block):
        super(ASTStateTask, self).__init__(service, block)

        if service.value not in self.valid_services and \
           service.value not in AWS_SERVICES.keys():
            service.raise_error('Invalid Task service')

        if function is None:
            if service.value in ('Lambda', 'Activity', 'Arn'):
                if name is None:
                    service.raise_error('{} task requires a function name argument'.format(service.value))

                function = name
                name = None
            else:
                service.raise_error('{} task requires a function to call'.format(service.value))
        else:
            if service.value in ('Lambda', 'Activity', 'Arn'):
                function.raise_error('Unexpected function name')
            else:
                try:
                    function.lookup = function.value # Save value for looking up when checking kwargs
                    function.value = AWS_SERVICES[service.value][function.value]['name']
                except KeyError:
                    function.raise_error('Invalid Task function')

        if name is not None:
            name.raise_error('Unexpected argument')

        if service.value == 'Arn' and not function.value.startswith('arn:aws:'):
            function.raise_error("ARN must start with 'arn:aws:'")
        if service.value in ('Lambda', 'Activity') and self.parameters is not None:
            tuple(self.parameters.keys())[0].raise_error('Unexpected keyword argument')

        if service.value not in ('Lambda', 'Activity', 'Arn'):
            required = AWS_SERVICES[service.value][function.lookup]['required_keys']
            required = copy.copy(required) # will be mutating to determine missing required arguments
            optional = AWS_SERVICES[service.value][function.lookup]['optional_keys']
            sync = AWS_SERVICES[service.value][function.lookup]['sync']

            if self.parameters:
                # self.parameters can be None either if no `parameters:` block is provided
                # or if there is a syntax error in the `parameters:` block
                for key in self.parameters.keys():
                    k = key.value
                    if k.endswith('.$'):
                        k = k[:-2] # remove the `.$`, which donates that the key uses a JsonPath

                    if k == 'sync':
                        sync = self.parameters[key]
                        if type(sync) != bool:
                            key.raise_error("Synchronous value must be a boolean")
                        del self.parameters[key]
                    elif k in required:
                        required.remove(k)
                    elif k not in optional:
                        key.raise_error("Invalid keyword argument")

            if sync == True:
                function.value += '.sync'

            if len(required) > 0:
                missing = ", ".join(required)
                function.raise_error("Missing required keyword arguments: {}".format(missing))

        self.service = service
        self.function = function

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

class ASTModIterator(ASTNode):
    """This modifier is used by the Map state."""
    name = 'Iterator'

    def __init__(self, iterator, block):
        comment, states = block
        super(ASTModIterator, self).__init__(iterator.token)
        self.block = states

class ASTModMaxConcurrency(ASTModKV):
    """
    This modifier is used by the Map state to limit number of instances of
    the iterator that may run in parallel.
    """
    name = 'MaxConcurrency'

class ASTModItemsPath(ASTModKV):
    """
    This modifier selects an array in the input.  Each element of this array is
    passed the iterator.
    """
    name = 'ItemsPath'

class ASTStateMap(ASTState):
    state_type = 'Map'
    valid_modifiers = [ASTModInput,
                       ASTModParameters,
                       ASTModResult,
                       ASTModOutput,
                       ASTModRetry,
                       ASTModCatch,
                       ASTModItemsPath,
                       ASTModIterator,
                       ASTModMaxConcurrency]

    def __init__(self, state, block, transform, error):
        comment, states = block

        super(ASTStateMap, self).__init__(state, (comment, states))

        if self.iterator is None:
            state.raise_error('Map state must have an iterator')

class ASTModVersion(ASTModKV):
    pass

class ASTStepFunction(ASTNode):
    def __init__(self, comment, version, timeout, states):
        super(ASTStepFunction, self).__init__() # ???: use the first states's token?
        self.comment = comment
        self.version = version
        self.timeout = timeout
        self.states = states

    def __repr__(self):
        return "\n".join(repr(state) for state in self.states)

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
                    state_ = ASTStatePass(state, None)
                    state_.name = state.name + "Loop"
                    state_.next = state.name
                    state.branches[key].append(state_)

        else:
            state.end = next_ is None
            state.next = next_

        if state.catch is not None:
            for catch in state.catch:
                states_ = catch.block
                linked_ = link(states_, final=next_)
                catch.next = linked_[0].name
                linked.extend(linked_)

        if state.iterator is not None:
            states_ = state.iterator.block
            state.iterator.states = link(states_, final=None)

        # Different states use the branches variable in different ways
        if isinstance(state, ASTStateChoice):
            for key in state.branches:
                states_ = state.branches[key]
                if isstr(states_):
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

    Args:
        branch (list): List of ASTState objects

    Raises:
        CompileError : If any of the checks fail
    """
    if not hasattr(branch, 'states'):
        branch.raise_error("Trying to check names for non-branch state")

    to_process = [branch.states]

    while len(to_process) > 0:
        states = to_process.pop(0)

        names = set() # State names are unique to the branch
        for state in states:
            if len(state.name) > MAX_NAME_LENGTH:
                state.raise_error("Name exceedes {} characters".format(MAX_NAME_LENGTH))

            if state.name in names:
                state.raise_error("Duplicate state name '{}'".format(state.name))

            names.add(state.name)

            if isinstance(state, ASTStateParallel):
                for branch in state.branches:
                    to_process.append(branch.states)
            elif isinstance(state, ASTStateMap):
                # Check the map state's internal state machine
                to_process.append(state.iterator.states)

def resolve_arns(branch, region = '', account_id = ''):
    """AST Transform that sets the `arn` attribute for ASTStateTasks

    Args:
        branch (list): List of ASTState objects
        region (str): AWS Region where the Lambdas / Activities reside
        account_id (str): AWS Account ID where the Lambdas / Activities reside
    """
    if not hasattr(branch, 'states'):
        branch.raise_error("Trying to resolve arns for non-branch state")

    for state in branch.states:
        if isinstance(state, ASTStateTask):
            if state.service.value == 'Arn':
                # ARN value already checked for 'arn:aws:' prefix in ASTStateTask constructor
                state.arn = state.function.value
            else:
                # arn:partition:service:region:account:task_type:name
                if state.service.value == 'Lambda':
                    service = 'lambda'
                    task_type = 'function'
                else:
                    service = 'states'
                    task_type = state.service.value.lower()
                if state.service.value not in ('Lambda', 'Activity'):
                    region = ''
                    account_id = ''
                parts = ['arn', 'aws',
                         service,
                         region,
                         account_id,
                         task_type,
                         state.function.value]
                state.arn = ":".join(parts)

        elif isinstance(state, ASTStateParallel):
            for branch in state.branches:
                resolve_arns(branch, region, account_id)
        elif isinstance(state, ASTStateMap):
            resolve_arns(state.iterator, region, account_id)

def verify_goto_targets(branch):
    """Recursivly checks that all Goto states target valid state names

    Valid state names are those states in the current branch. This means that
    a Goto cannot target states in another parallel branch or from a parallel
    branch to the main body of the Step Function

    Args:
        branch (list): List of ASTState objects

    Raises:
        CompileError : If a Goto state targets an invalid state
    """
    if not hasattr(branch, 'states'):
        branch.raise_error("Trying to check goto targets for non-branch state")

    to_process = [branch.states]

    while len(to_process) > 0:
        states = to_process.pop(0)

        names = set() # Need to know all of the valid state names for the branch
        for state in states:
            names.add(state.name)

            if isinstance(state, ASTStateParallel):
                for branch in state.branches:
                    to_process.append(branch.states)
            elif isinstance(state, ASTStateMap):
                # Check the map state's internal state machine
                to_process.append(state.iterator.states)

        for state in states:
            if isinstance(state.next, ASTModNext):
                if state.next.value not in names:
                    state.next.raise_error("Goto target '{}' doesn't exist".format(state.next.value))

class StateVisitor(object):
    """Generic base class for heaviside users to create a visitor that can modify
    ASTStateTasks
    """

    def dispatch(self, state):
        """Dispatch the given state to the approprate handler function

        Args:
            state (ASTState): State to dispatch
        """

        if isinstance(state, ASTStateTask):
            self.handle_task(state)

    def visit(self, branch):
        """Visit all states in all branches of the state machine and dispatch
        them to be handled the subclass

        Args:
            branch (list): List of ASTState objects
        """
        if not hasattr(branch, 'states'):
            raise ValueError("Trying to visit non-branch state: {}".format(branch))

        for state in branch.states:
            self.dispatch(state)

            if isinstance(state, ASTStateParallel):
                for branch in state.branches:
                    self.visit(branch)
            elif isinstance(state, ASTStateMap):
                self.visit(state.iterator)

    def handle_task(self, state):
        """ASTStateTask handler function placeholder

        Args:
            state (ASTStateTask): State to handle
        """
        pass
