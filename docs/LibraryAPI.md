# Heaviside Library API

This document describes the public API available to other developers using the Heaviside library.

    Copyright 2019 The Johns Hopkins University Applied Physics Laboratory

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.


## Table of Contents:

* [Overview](#Overview)
* [Utilities](#Utilities)
  - [Path](#Path)
* [Compiling](#Compiling)
  - [Visitors](#Visitors)
* [State Machine](#State-Machine)
  - [Add Visitor](#Add-Visitor)
  - [Build](#Build)
  - [Create](#Create)
  - [Update](#Update)
  - [Delete](#Delete)
  - [Start](#Start)
  - [Stop](#Stop)
  - [Status](#Status)
  - [Wait](#Wait)
  - [Running ARNs](#Running-ARNs)
* [Activities](#Activities)
  - [Activity Mixin](#Activity-Mixin)
  - [Task Mixin](#Task-Mixin)
  - [Activity Object](#Activity-Object)
  - [Task Process](#Task-Process)
  - [Activity Process](#Activity-Process)
  - [Activity Manager](#Activity-Manager)
  - [Fan-out](#Fan-out)

## Overview

Heaviside provides both a command line interface as well as a Python library. More information on the command line interface is in the [README](../README.md).

This document covers what is considered the public API for Heaviside, meaning that any changes to these APIs will either be backwards compatible or will result in a major version number change. All of the APIs described contain full Python documentation with more details about arguments and functionality.

## Utilities

Heaviside utility functions.

### Path

Heaviside makes use of `pathlib.Path` to be able to determine if the given argument is a file path reference or if it is a string containing the data to work with. Because `pathlib` was added in Python 3.4 Heaviside includes a minimal implementation that covers just the features used internally by Heaviside.

This minimal implementation is only used if the current runtime doesn't have `pathlib`. If `pathlib.Path` is available it is exposed as `heaviside.utils.Path` so that code can be interoperable between different Python version.

```python
source = heaviside.utils.Path('/path/to/file.hsd')
# or
source = heaviside.utils.Path() / 'path' / 'to' / 'file.hsd'
```

For more details refer to the [pathlib](https://docs.python.org/3.4/library/pathlib.html) documentation.

## Compiling

The core of Heaviside is the [Heaviside DSL](StepFunctionDSL.md) to [Step Function Definition](https://aws.amazon.com/step-functions/) compiler. The `compile` function compiles the given DSL source and returns the Step Function result.

`definition = heaviside.compile(heaviside.utils.Path('/path/to/hsd/file.hsd'))`

```python
heaviside.compile(source : Union[str, IOBase, Path],
                  region : str = '',
                  account_id : str = '',
                  visitors : List[Visitor] = [],
                  **kwargs) -> str
```

* `source`: The Heaviside DSL description to compile
            `Path` is either `pathlib.Path` or `heaviside.utils.Path`
* `region`: The name of the AWS region where Lambdas and Activities are located
* `account_id`: The AWS account ID where Lambdas and Activities are located
* `visitors`: A list of [Visitor](#Visitors) objects to apply during the compiling process
* `kwargs`: Additional key-word arguments to be passed to `json.dumps`
* Returns: String containing StepFunction Machine Definition

### Visitors

To support programatic modification of the Heaviside AST during compilation Heaviside supports a visitor pattern. This allows modifications like modifying the task ARN, allowing the creation of a generic Heaviside file that can be used in development, testing, and production deployments.

The `heaviside.ast.StateVisitor` class can serve as the base class for a custom visitor:

```python
class CustomVisitor(heaviside.ast.StateVisitor):
    def handle_task(self, task: heaviside.ast.ASTStateTask):
        pass
```

It is also possible to create a completely custom visitor, giving more control and ability but at the expense of needing to walk the branches of execution manually:

```python
class CustomVisitor(object):
    def visit(self, branch: List[heaviside.ast.ASTState]):
        # NOTE: only called one with the main branch of execution and it is the callee's reposibility
        #       to decent into the nested branches of execution
        pass
```

## State Machine

A Step Function is more than then definition of the state machine but also includes the launching different executions and the lifecycle of the executions and the Step Function itself. The `heaviside.StateMachine` object makes managing this lifecycle easier and more convenient.

Note that when initializing a new `heaviside.StateMachine` object it will query AWS to determine if the Step Function exists and if so the ARN of the Step Function.

`machine = heaviside.StateMachine('StepFunction_Name')`

```python
heaviside.StateMachine(name : str,

                       # Only provide one of the following or allow Boto3 to
                       # search for credentials on the system

                       # Existing Boto3 Session object
                       session : boto3.session.Session,

                       # JSON object, String with JSON object, File with JSON object
                       # containing a Secret Key and Access Key (and optional
                       # Account ID and Region
                       credentials : Union[dict, IOBase, Path, str],

                       # Individual keyword arguments
                       # AWS Secret Key
                       secret_key : str,
                       # or
                       aws_secret_key: str,

                       # AWS Access Key
                       access_key: str,
                       # or
                       aws_access_key: str,

                       # Account ID (will be looked up if not provided)
                       account_id: str,
                       # or
                       aws_account_id: str,

                       # Region (will be looked up from EC2 Meta-data if not provided)
                       region: str,
                       # or
                       aws_region: str
                       
                       ) -> StateMachine
```

* `name`: The name of the Step Function to manage
* `kwargs`: Arguments passed to `heaviside.utils.create_session` to setup a Boto3
            Session for communicating with AWS

### Add Visitor

Add a custom `heaviside.ast.StateVisitor` to be used when building or creating a new Step Function.

`machine.add_visitor(heaviside.ast.StateVisitor())`

```python
StateMachine.add_visitor(visitor: heaviside.ast.StateVisitor) -> None
```

* `visitor`: A custom implementation / subclass of `heaviside.ast.StateVisitor`

### Build

Compile a given Heaviside DSL definition. This is just a wrapper around `heaviside.compile` that passess the `heaviside.StateMachine` variables. This is normally used by `StateMachine.create`.

`definition = machine.build(heaviside.utils.Path('/path/to/hsd/file.hsd'))`

```python
StateMachine.build(source: Union[str, IOBase, Path],
                   **kwargs) -> str
```

* `source`: The Heaviside DSL description to compile
            `Path` is either `pathlib.Path` or `heaviside.utils.Path`
* `kwargs`: Additional key-word arguments to be passed to `json.dumps`

### Create

Compile the given Heaviside DSL definition and then create the Step Function in AWS.

`arn = machine.create(heaviside.utils.Path('/path/to/hsd/file.hsd'))`

```python
StateMachine.create(source: Union[str, IOBase, Path],
                    role: str) -> arn
```

* `source`: The Heaviside DSL description to compile
            `Path` is either `pathlib.Path` or `heaviside.utils.Path`
* `role`: AWS IAM role for the State Machine to execute under
          This can either be the full ARN or just the IAM role name
* Returns: ARN of the Step Function

### Update

Update the Heaviside DSL definition and / or the IAM role of the Step Function in AWS

`machine.update(heaviside.utils.Path('/path/to/hsd/file.hsd'))`

```python
StateMachine.create(source: optional[Union[str, IOBase, Path]],
                    role: optional[str]) -> None
```

* `source`: The Heaviside DSL description to compile
            `Path` is either `pathlib.Path` or `heaviside.utils.Path`
* `role`: AWS IAM role for the State Machine to execute under
          This can either be the full ARN or just the IAM role name
 
### Delete

Delete the AWS Step Function definition. After this the Step Function is no longer usable.

`machine.delete()`

```python
StateMachine.delete(exception: bool = False) -> None
```

* `exception`: If a `HeavisideError` should be raised if the Step Function doesn't exist

### Start

Start the execution of a Step Function.

`arn = machine.start({'key1': 'value1', 'key2': 'value2'})`

```python
StateMachine.start(input_: object,
                   name: Optional[str]) -> str
```

* `input_`: The input JSON text to pass to the start state
* `name`: The name of the Step Function execution, if `None` then one will be generated
* Returns: ARN of the Step Function execution

### Stop

Stop the execution of an active Step Function execution.

`machine.stop('arn:aws:...', 'Error Message', 'Cause Details')`

```python
StateMachine.stop(arn: str,
                  error: str,
                  cause: str) -> None
```

* `arn`: The ARN of the running execution to terminate
* `error`: The error message to report for the termination
* `cause`: The error cause details to report for the termination

### Status

Get the current status of an active Step Function execution.

`status = machine.status('arn:aws:...')`

```python
StateMachine.status(arn: str) -> str
```

* `arn`: The ARN of the running execution to get the status of
* Returns: One of the following values: `RUNNING`, `SUCCEEDED`, `FAILED`, `TIMED_OUT`, `ABORTED`

### Wait

Wait until an active Step Function execution has finished executing and get the output or the error information.

`machine.wait(arn)`

```python
StateMachine.wait(arn: str,
                  period : int = 10) -> Dict
```

* `arn`: The ARN of the running execution to monitor
* `period`: The number of seconds to sleep between polls for status
* Returns: Step Function output or error event details

### Running ARNs

Get a list of the ARNs for the running executions for the Step Function.

`arns = machine.running_arns()`

```python
StateMachine.running_args() -> List[str]
```

* Returns: List of ARNs for running executions of the named Step Function

## Activities

AWS Step Function Activities are workers implemented and hosted by the user and are used to perfom a specific task. This gives the user a lot of flexibility, but with the trade off of more management. Activites are helpful if the task is longer running or requires more resources then a Lambda can handle.

Heaviside provides several classes, detailed below, that support creating Activity workers and responding to Step Function requests. The majority of the logic is provided in the form of two mixin classes, one for activities (polling for tasking) and one for tasks (processing input and sending AWS the results or a failure), making it easier to integrate the functionality into other code. In addition to the mixins there are several classes, that use the mixins, that form the bases for a service that manages multiple activities and their worker processes.

All of the Heaviside activity code is configured to use the Python `logging` module with the `heaviside.activities` prefix.

### Activity Mixin
The main job of the ActivityMixin is to provide a method (`ActivityMixin.run()`) that polls the given Activity ARN for tasking. When received, it passes it to a method (`handle_task()`) that will actually process the request and send the response. The method handle_task is not defined in the mixin.

If the method handle_task returns a non-None value, it is expected to be a Thread like object (with `start()` and `is_alive()` methods). ActivityMixin.run will start the Thread like object and keep track of how many objects are concurrently running. An upper bound can be placed on this number, and if it is reached, ActivityMixin.run will not accept any new tasking until a Thread like object has finished execution.

### Task Mixin
The main job of the TaskMixin is to provide a wrapper around a method that processes the task input and produces an output. The wrapper takes care of sending the output back to AWS or capturing any exceptions and sending the failure back to AWS.

The TaskMixin also supports sending a heartbeat when the Step Function expects it. The TaskMixin handles sending heartbeats by using generator coroutines to ceed control from the executing code back to the TaskMixin. The target code uses 'yield' to ceed execution and send a heartbeat. After the heartbeat has been sent, the coroutine resumes execution where it left offf.

```python
def example_heartbeat(input_):
    for i in input_:
        yield # send heartbeat
        # process i

    # Python 3
    return output # whatever data to pass to the next step

    # Python 2
    yield output # whatever data to pass to the next step
```

### Activity Object

The Activity object (`heaviside.activities.Activity`) implements both mixins, making it easy to embed activity processing into existing code or framework.

### Task Process

The TaskProcess object (`heaviside.activities.TaskProcess`) implements `heaviside.activities.TaskMixin` and `multiprocessing.Process` and is responsible for executing the given function and handling the results in a seperate Python process.

### Activity Process

The ActivityProcess object (`heaviside.activities.ActivityProcess`) implements the `heaviside.activities.ActivityMixin` and `multiprocessing.Process` and is responsible for monitoring an Activity ARN for tasking and spawning a TaskProcess to handle the task, while it continues to monitor for more work.

### Activity Manager

The ActivityManager (`heaviside.activities.ActivityManager`) is used to launch multiple ActivityProcesses and monitor them to ensure that they havn't crashed.  If they have, it will start a new ActivityProcess.

The class is designed to be subclassed to provide the list of ActivityProcesses that should be launched an monitored

#### Example Activity Manager

```python
from heaviside.activities import ActivityManager

def example_activity(input_):
    # process input data
    return input_

class ExampleActivityManager(ActivityManager):
    def __init__(self, **kwargs):
        super(ExampleActivityManager, self).__init__(**kwargs)

        # Dictionary of activity name : activity function
        self.activities = {
            'example': example_activity,
        }

if __name__ == '__main__':
    manager = ExampleActivityManager()
    manager.run()
```

### Fan-out
Currently AWS Step Functions do not support dynamic parallel execution of
tasks. As a temporary solution there is a function `heaviside.activities.fanout`
that helps execute a dynamic number of step functions and collect the results.
This requires splitting the desired functionality into a seperate step function,
but provides an easy to implement solution.

The fanout function will execute the given step function once per input argument
and poll the execution ARN for the results. If successful, returns are
aggregated into any array to be returned. If a failure occures, the failure
information is extracted and raised as a `heaviside.exceptions.ActivityError`.
If any exception is raised, fanout will attempt to stop any executing step
functions before returning, so there are no orphaned executions running.

The fanout function will also limit the number of concurrently executions and
will slowly ramp up the execution of the step functions. The ramp up allows for
other AWS resources to scale before the total number of executing processes
are running.

The fanout function also has two additional delay arguments that can be used to
limit the rate at which AWS requests are made during status polling. These can be
changed to keep the caller from exceeding the AWS throttling limits (200 unit
bucket per account, refilling at 1 unit per second).

**Note:** The fanout function currently maintains state in-memory, which means
that if there is a failure, state / sub-state is lost. It also means that fanout
works best when calling stateless / idempotent step functions or when the caller
can clean up state before a retry is attempted
