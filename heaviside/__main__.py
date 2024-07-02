# Copyright 2024 The Johns Hopkins University Applied Physics Laboratory
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

import os
import sys
import json
import argparse
import heaviside

# heaviside.utils handles either importing pathlib
# or creating a custom Path object (for Python 2.7+)
Path = heaviside.utils.Path


def main():
    parser = argparse.ArgumentParser(
        description="Heaviside CLI script for executing the Heaviside Python Library",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--credentials",
        "-c",
        metavar="<file>",
        default=os.environ.get("AWS_CREDENTIALS"),
        type=argparse.FileType("r"),
        help="File with credentials to use when connecting to AWS (default: AWS_CREDENTIALS)",
    )
    parser.add_argument(
        "--region",
        "-r",
        metavar="<aws_region>",
        default=os.environ.get("AWS_REGION"),
        help="AWS Region (default: AWS_REGION)",
    )
    parser.add_argument(
        "--account_id",
        "-a",
        metavar="<aws_account_id>",
        default=os.environ.get("AWS_ACCOUNT_ID"),
        help="AWS Account ID (default: AWS_ACCOUNT_ID)",
    )
    parser.add_argument(
        "--secret_key",
        metavar="<aws_secret_key>",
        default=os.environ.get("AWS_SECRET_KEY"),
        help="AWS Secret Key (default: AWS_SECRET_KEY)",
    )
    parser.add_argument(
        "--access_key",
        metavar="<aws_access_key>",
        default=os.environ.get("AWS_ACCESS_KEY"),
        help="AWS Access Key (default: AWS_ACCESS_KEY)",
    )

    subparsers = parser.add_subparsers(
        dest="command", metavar="<command>", help="Command to execute"
    )

    #### compile ####
    compile_parser = subparsers.add_parser(
        "compile", help="Compile a Heaviside file into a StepFunction State Machine"
    )
    compile_parser.add_argument(
        "--output",
        "-o",
        metavar="<file>",
        default="-",
        help="Location to save the StepFunction State Machine to (default: stdout)",
    )
    compile_parser.add_argument("file", help="heaviside file to compile")

    #### create ####
    create_parser = subparsers.add_parser(
        "create", help="Compile a Heaviside file and create a AWS StepFunction"
    )
    create_parser.add_argument(
        "--name", "-n", metavar="<name>", help="StepFunction name (default: filename)"
    )
    create_parser.add_argument("file", help="heaviside file to compile and create")
    create_parser.add_argument(
        "role", help="AWS IAM role name or full ARN of the role for the StepFunction"
    )

    #### update ####
    update_parser = subparsers.add_parser(
        "update", help="Compile a Heaviside file and create a AWS StepFunction"
    )
    update_parser.add_argument("name", help="Name of the StepFunction to update")
    update_parser.add_argument(
        "--file", "-f", help="heaviside file to compile and update"
    )
    update_parser.add_argument(
        "--role",
        "-r",
        help="AWS IAM role name or full ARN of the role for the StepFunction",
    )

    #### delete ####
    delete_parser = subparsers.add_parser("delete", help="Delete an AWS StepFunction")
    delete_parser.add_argument("name", help="Name of the StepFunction to delete")

    #### start ####
    start_parser = subparsers.add_parser(
        "start", help="Start executing a AWS StepFunction"
    )
    start_parser.add_argument(
        "--no-wait",
        "-n",
        dest="wait",
        action="store_false",
        help="If the command should wait for the StepFunction to finish execution",
    )
    start_parser.add_argument(
        "--input",
        "-i",
        help="Path to file containing input Json data for the StepFunction ('-' for stdin)",
    )
    start_parser.add_argument(
        "--json", "-j", help="Json input data for the StepFunction"
    )
    start_parser.add_argument(
        "name", help="Name of the StepFunction to start executing"
    )

    args = parser.parse_args()

    if args.command is None:
        parser.print_usage()
        name = os.path.basename(sys.argv[0])
        print("{} error: the following arguments are required: command".format(name))
        return 1

    credentials = {}
    if args.credentials is not None:
        credentials["credentials"] = json.load(args.credentials)
    if args.region is not None:
        credentials["region"] = args.region
    if args.account_id is not None:
        credentials["account_id"] = args.account_id
    if args.secret_key is not None:
        credentials["secret_key"] = args.secret_key
    if args.access_key is not None:
        credentials["access_key"] = args.access_key

    try:
        if args.command == "compile":

            def find_(key):
                if key in credentials:
                    return credentials[key]
                if "credentials" in credentials:
                    for key_ in (key, "aws_" + key):
                        if key_ in credentials["credentials"]:
                            return credentials["credentials"][key_]
                print("Could not find {} value, using ''".format(key), file=sys.stderr)
                return ""

            region = find_("region")
            account = find_("account_id")

            machine = heaviside.compile(Path(args.file), region, account, indent=3)

            with heaviside.utils.write(args.output) as fh:
                fh.write(machine)
        elif args.command == "create":
            name = args.name
            if name is None:
                name = os.path.basename(args.file)
                name = os.path.splitext(name)[0]

            machine = heaviside.StateMachine(name, **credentials)
            machine.create(Path(args.file), args.role)
        elif args.command == "update":
            if args.file:
                args.file = Path(args.file)

            machine = heaviside.StateMachine(args.name, **credentials)
            machine.update(args.file, args.role)
        elif args.command == "delete":
            machine = heaviside.StateMachine(args.name, **credentials)
            machine.delete(True)
        elif args.command == "start":
            if args.json is not None:
                input_ = args.json
            elif args.input is not None:
                if args.input == "-":
                    input_ = sys.stdin.read()
                else:
                    with open(args.input, "r") as fh:
                        input_ = fh.read()
            else:
                input_ = "{}"
            input_ = json.loads(input_)

            machine = heaviside.StateMachine(args.name, **credentials)
            arn = machine.start(input_)

            if args.wait:
                output = machine.wait(arn)
                print(json.dumps(output, indent=3))
            else:
                print("Execution ARN: {}".format(arn))
        else:
            raise heaviside.exceptions.HeavisideError(
                "Unsupported command: {}".format(args.command)
            )
    except heaviside.exceptions.HeavisideError as e:
        print(e)
        return 1
    except Exception as e:
        print("Error: {}".format(e))
        return 1
    return 0
