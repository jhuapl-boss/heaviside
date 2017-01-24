# AWS Step Function Activities

This document describes the support utilities that the Heaviside includes for
Step Function Activities.

## Table of Contents:

* [Activity Object](#Activity-Object)
* [Activity Processes](#Activity-Processes)
  - [TaskProcess](#TaskProcess)
  - [ActivityProcess](#ActivityProcess)
  - [ActivityManager](#ActivityManager)
    - [Example Activity Manager](#Example-Activity-Manager)

## Activity Object

The Activity object (`heaviside.activities.Activity`) makes it easy to embed
activity processing into existing code or framework. The object wraps the calls
to AWS and handles Json data serialization.

## Activity Processes

Heaviside provides three classes that can be used to create a multi-process
service (or process) that will monitor multiple activites for tasking. These
classes are subclasses of the Python `multiprocessing.Process` class.

### TaskProcess

The TaskProcess object (`heaviside.activities.TaskProcess`) is responsible for
taking the input to an AWS Activity invocation, processing the request, and
returning a response. The implementation of the task's logic can either be
passed into as a function to call, or TaskProcess can be subclassed to include
the logic.

Activities can send heartbeats back to the StepFunction. This is only required
when the StepFunction expects heartbeats. TaskProcess handles sending heartbeats
by using generator coroutines to ceed control from the task processing code back
to the TaskProcess. The target code uses `yield` to return control to send a
heartbeat. After the heartbeat has been sent, the coroutine resumes execution
where it left off.

```python
def example_heartbeat(input_):
    for i in input_:
        yield # send heartbeat
        # process i
```

### ActivityProcess

The ActivityProcess object (`heaviside.activities.ActivityProcess`) is
responsible for monitoring an Activity ARN for tasking and spawning a
TaskProcess to handle the task, while it continues to monitor for more
work.

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
    def build(self):
        def dispatch(target):
            def wrapped(*args, **kwargs):
                return TaskProcess(*args, target=target, **kwargs)
            return wrapped

        return [
            lambda: ActivityProcess('example', dispatch(example_activity)),
        ]

if __name__ == '__main__':
    manager = ExampleActivityManager()
    manager.run()
```
