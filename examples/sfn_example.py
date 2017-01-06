#!/usr/bin/env python3

import sys
import os
import argparse
from threading import Thread
from pathlib import Path

import alter_path
from lib.stepfunctions import StateMachine, Activity

sfn = """
Activity('Echo')
Wait(seconds = 5)
Activity('Echo')
"""

class BossStateMachine(StateMachine):
    def __init__(self, name, domain, *args, **kwargs):
        name_ = name + "." + domain
        name_ = ''.join([x.capitalize() for x in name_.split('.')])
        super().__init__(name_, *args, **kwargs)
        self.domain = domain

    def _translate(self, function):
        return "{}.{}".format(function, self.domain)

def run_activity(domain, count, credentials):
    activity = Activity('Echo.' + domain, credentials = credentials)

    activity.create()
    try:
        while count > 0:
            input_ = activity.task()
            if input_ is None:
                continue
            count -= 1

            print("Echo: {}".format(input_))

            activity.success(input_)
    except Exception as e:
        print("Error: {}".format(e))
        raise
    finally:
        activity.delete()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = "Example AWS Step Function script")
    parser.add_argument("--aws-credentials", "-a",
                        metavar = "<file>",
                        default = os.environ.get("AWS_CREDENTIALS"),
                        help = "File with credentials to use when connecting to AWS (default: AWS_CREDENTIALS)")
    parser.add_argument("domain_name", help="Domain in which to execute the configuration (example: subnet.vpc.boss)")

    args = parser.parse_args()

    if args.aws_credentials is None:
        parser.print_usage()
        print("Error: AWS credentials not provided and AWS_CREDENTIALS is not defined")
        sys.exit(1)

    credentials = Path(args.aws_credentials)
    domain = args.domain_name

    activity = Thread(target = run_activity, args = (domain, 2, credentials))
    activity.start()

    machine = BossStateMachine('hello.world', domain, credentials = credentials)
    if machine.arn is None:
        role = "StatesExecutionRole-us-east-1"
        machine.create(sfn, role)
    else:
        for arn in machine.running_arns():
            macine.stop(arn, "USER", "Script automatically stops old executions")

    args = {"input": "Hello World!"}
    print("Input: {}".format(args))
    arn = machine.start(args)
    output = machine.wait(arn)
    print("Output: {}".format(output))

    machine.delete()
    activity.join()
