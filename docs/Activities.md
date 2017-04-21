# AWS Step Function Activities

This document describes the support utilities that the Heaviside includes for
Step Function Activities.

## Table of Contents:

* [Logging](#Logging)
* [Mixins](#Mixins)
  - [ActivityMixin](#ActivityMixin)
  - [TaskMixin](#TaskMixin)
* [Activity Object](#Activity-Object)
* [Activity Processes](#Activity-Processes)
  - [TaskProcess](#TaskProcess)
  - [ActivityProcess](#ActivityProcess)
  - [ActivityManager](#ActivityManager)
    - [Example Activity Manager](#Example-Activity-Manager)
* [Fan-out](#Fan-out)

## Logging
All activities code uses the Python logging module with the name
`heaviside.activities`. Information about errors caught will be logged and can
provide a developer or operator information about problems with activities.

## Mixins
The majority of logic is provided in the form of two mixin classes, one for
activities (polling for tasking) and one for tasks (processing input and
sending AWS the results or a failure)

### ActivityMixin
The main job of the ActivityMixin is to provide a method (`ActivityMixin.run()`)
that polls the given Activity ARN for tasking. When received, it passes it to 
a method (`handle_task()`) that will actually process the request and send the
response. The method handle_task is not defined in the mixin.

If the method handle_task returns a non-None value, it is expected to be a
Thread like object (with `start()` and `is_alive()` methods). ActivityMixin.run
will start the Thread like object and keep track of how many objects are
concurrently running. An upper bound can be placed on this number, and if
it is reached, ActivityMixin.run will not accept any new tasking until a
Thread like object has finished execution.

### TaskMixin
The main job of the TaskMixin is to provide a wrapper around a method that
processes the task input and produces an output. The wrapper takes care of
sending the output back to AWS or capturing any exceptions and sending the
failure back to AWS.

The TaskMixin also supports sending a heartbeat when the Step Function expects
it. The TaskMixin handles sending heartbeats by using generator coroutines to
ceed control from the executing code back to the TaskMixin. The target code
uses 'yield' to ceed execution and send a heartbeat. After the heartbeat has
been sent, the coroutine resumes execution where it left offf.

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

## Activity Object

The Activity object (`heaviside.activities.Activity`) implements both mixins,
making it easy to embed activity processing into existing code or framework.

## Activity Processes

Heaviside provides three classes that can be used to create a multi-process
service (or process) that will monitor multiple activites for tasking. These
classes are subclasses of the Python `multiprocessing.Process` class.

### TaskProcess

The TaskProcess object (`heaviside.activities.TaskProcess`) implements the
TaskMixin and is responsible for executing the given function and handling
the results in a seperate Python process.

### ActivityProcess

The ActivityProcess object (`heaviside.activities.ActivityProcess`) implements
the ActivityMixin and is responsible for monitoring an Activity ARN for tasking
and spawning a TaskProcess to handle the task, while it continues to monitor
for more work.

### ActivityManager

The ActivityManager (`heaviside.activities.ActivityManager`) is used to launch
multiple ActivityProcesses and monitor them to ensure that they havn't crashed.
If they have, it will start a new ActivityProcess.

The class is designed to be subclassed to provide the list of ActivityProcesses
that should be launched an monitored

#### Example Activity Manager

```python
from heaviside.activities import ActivityManager, ActivityProcess, TaskProcess

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

## Fan-out
Currently AWS Step Functions do not support dynamic parallel execution of
tasks. As a temporary solution there is a function `heaviside.activities.fanout`
that helps execute a dynamic number of step functions and collect the results.
This requires splitting the desired functionality into a seperate step function,
but provides and easy to implement solution.

The fanout function will execute the given step function once per input argument
and poll the execution ARN for the results. If successful, returns are
aggregated into any array to be returned. If a failure occures, the failure
information is extracted and raised as a `heaviside.exceptions.ActivityException`.

The fanout function will also limit the number of concurrently executions and
will slowly ramp up the execution of the step functions. The ramp up allows for
other AWS resources to scale before the total number of executing processes
are running.

