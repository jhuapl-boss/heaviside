# Heaviside
Heaviside is a domain specific language (DSL) and Python compiler / support
libraries for working with [AWS StepFunctions].

## Why
The reason for a StepFunctions DSL is that the [state machine language], while
flexible, is hard to write and maintain. The DSL 

## DSL

The document [docs/StepFunctionDSL.md] describes the Heaviside DSL.

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

The Heaviside package installs the Python library `heaviside`. There are three
components to the library.

* `heaviside.compile`: The method used to compile a Heaviside DSL file into
                       a AWS StepFunction.
* `heaviside.StateMachine`: Class for creating and executing StepFunctions.
* `heaviside.activities`: A submodule containing several helper classes providing
                          a framework for running StepFunction Activities.
                          See [docs/Activities.md] for more information.

## Compatability

Currently Heaviside has only been tested with Python 3.5. In the future this
will be expanded to hopefully include Python 2 compatability.


[AWS StepFunctions]: https://aws.amazon.com/step-functions/
[state macine language]: https://state-language.net/spec.html
