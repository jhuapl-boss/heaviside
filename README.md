# Heaviside
Heaviside is a domain specific language (DSL) and Python compiler / support
libraries for working with [AWS StepFunctions].

## Why
The reason for a StepFunctions DSL is that the [state machine language], while
flexible, is hard to write and maintain. The DSL provides a simplied format for
writing StepFunction state machines and serveral of common flow control
statements.

## DSL

The [StepFunctionDSL] document describes the Heaviside DSL.

## Getting Started

In this document `.hsd` will be used to denote a StepFunction file written in
the Heaviside DSL. The extension `.sfn` will be used to denote a StepFunction
file written in the AWS [state machine language].

### Installing

```
pip install heaviside
```

### CLI Script

The Heaviside package installs a script called `heaviside` that provides a CLI
to the library. Running the command without arguments or with the `--help` or
`-h` flag will provide detailed help.

#### AWS Credentials

All sub-commands (except `compile`) connect to AWS to manipulate StepFunctions.
There are multiple ways to define the AWS credentials, listed in the order of
precedence.

* Explicitly pass the values as command line arguments or environmental variables
* Pass a file path to a JSON file with the arguments
* Letting Boto3 try to resolve secret / access keys
* Looking at EC2 meta data for current AWS region
* Looking at current IAM user for AWS account_id

The `compile` sub-command doesn't connect to AWS, but does use the region and
account_id values when resolving Task ARNs. If the Heaviside DSL file has full
Task ARNs or the compiled file will not be uploaded to AWS these values can be
blank.

**Note**: Since `compile` doesn't connect to AWS, only the first two options in
the list above are valid for passing the region and account_id value.

#### Compiling

To compile a Heaviside file into a AWS StepFunction file use the `compile`
sub-command.

```
$ heaviside compile state_machine.hsd -o state_machine.sfn 
```

#### Creating a StepFunction

The `heaviside` script can compile and upload the resulting file to AWS.

```
$ heaviside create state_machine.hsd AwsIamStepFunctionRole
```

Arguments:
* `state_machine.hsd`: The path to the Step Function definition written in the
                       Heaviside DSL.
* `AwsIamStepFunctionRole`: The AWS IAM Role that the StepFunction will use
                            when executing. Most often this will be used to
                            control which Lambdas and Activities the
                            StepFunction has permission to execute.

#### Updating a StepFunction

The `heaviside` script can update an existing Step Function definition and / or
IAM role in AWS.

```
$ heaviside update state_machine --file  state_machine.hsd --role AwsIamStepFunctionRole
```

Arguments:
* `state_machine`: Name of the state machine to update
* `state_machine.hsd`: The path to the Step Function definition written in the
                       Heaviside DSL.
* `AwsIamStepFunctionRole`: The AWS IAM Role that the StepFunction will use
                            when executing. Most often this will be used to
                            control which Lambdas and Activities the
                            StepFunction has permission to execute.

#### Deleting a StepFunction

The `delete` sub-command can be used to delete an existing StepFunction

```
$ heaviside delete state_machine
```

#### Executing a StepFunction

The `start` sub-command can be used to start executing a StepFunction.

```
$ heaviside start --json "{}" state_machine
```

**Note**: By default the `start` sub-command will wait until the
execution has finished and will print the output of the StepFunction.

## Python Library

The Heaviside package installs the Python library `heaviside`. The public
API is documented in the [Library API](docs/LibraryAPI.md) file.

## Compatibility

Currently, Heaviside has only been tested with Python 3.8 and 3.11

## Related Projects

* [statelint]: A Ruby project that verifies a AWS StepFunction definition file.
               Includes checks like making sure that all states are reachable.
               Helpful when developing a new StepFunction to ensure everything
               is correct. It will catch structural problems that Heaviside
               doesn't check for.


[AWS StepFunctions]: https://aws.amazon.com/step-functions/
[state machine language]: https://states-language.net/spec.html
[StepFunctionDSL]: docs/StepFunctionDSL.md
[Activities document]: docs/Activites.md
[statelint]: https://github.com/awslabs/statelint

## Legal

Use or redistribution of the Boss system in source and/or binary forms, with or without modification, are permitted provided that the following conditions are met:
 
1. Redistributions of source code or binary forms must adhere to the terms and conditions of any applicable software licenses.
2. End-user documentation or notices, whether included as part of a redistribution or disseminated as part of a legal or scientific disclosure (e.g. publication) or advertisement, must include the following acknowledgement:  The Boss software system was designed and developed by the Johns Hopkins University Applied Physics Laboratory (JHU/APL). 
3. The names "The Boss", "JHU/APL", "Johns Hopkins University", "Applied Physics Laboratory", "MICrONS", or "IARPA" must not be used to endorse or promote products derived from this software without prior written permission. For written permission, please contact BossAdmin@jhuapl.edu.
4. This source code and library is distributed in the hope that it will be useful, but is provided without any warranty of any kind.


