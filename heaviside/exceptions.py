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

# DP ???: Should this be a subclass of HeavisideError?
class ActivityError(Exception):
    def __init__(self, error, cause):
        super(ActivityError, self).__init__(cause)
        self.error = error
        self.cause = cause

    def __str__(self):
        return "{}: {}".format(self.error, self.cause)

class HeavisideError(Exception):
    """Base class for all Heaviside errors"""
    pass

class CompileError(HeavisideError):
    """A syntax error was encountered when compiling a Heaviside file

    Printing a CompileError will produce a message similar to a
    Python SyntaxError.
    """

    def __init__(self, lineno, pos, line, msg):
        """Args:
            lineno (int): Line number of the error
            pos (int): Position in the line where the error starts
            line (string): The line of the Heaviside file where the problem is
            msg (string): Syntax error message
        """
        super(CompileError, self).__init__(msg)
        self.source = '<unknown>'
        self.lineno = lineno
        self.pos = pos
        self.line = line
        self.msg = msg

    def __str__(self):
        # produce a Python style Syntax Error for display
        lines = []
        lines.append('File "{}", line {}'.format(self.source, self.lineno))
        # Note: Using format because of Python 2.7 string types
        lines.append('{}'.format(self.line))
        lines.append((' ' * self.pos) + '^')
        lines.append('Syntax Error: {}'.format(self.msg))
        return '\n'.join(lines)

    @classmethod
    def from_token(cls, tok, msg):
        lineno, pos = tok.start
        line = tok.line.rstrip() # remove newline
        return cls(lineno, pos, line, msg)

    @classmethod
    def from_tokenize(cls, error):
        msg, (lineno, pos) = error.args

        return cls(lineno, pos, '', msg)

