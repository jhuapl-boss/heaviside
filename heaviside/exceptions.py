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

class StepFunctionError(Exception):
    """Base class for all library errors"""

    def __init__(self, lineno, pos, msg, line=''):
        super(StepFunctionError, self).__init__(msg)
        self.lineno = lineno
        self.pos = pos
        self.line = line

class TokenError(StepFunctionError):
    @classmethod
    def from_tokenize(cls, error):
        msg, (lineno, pos) = error.args

        return cls(lineno, pos, msg)

class ParserError(StepFunctionError):
    @classmethod
    def from_token(cls, tok, msg):
        lineno, pos = tok.start
        line = tok.line.rstrip() # remove newline
        return cls(lineno, pos, msg, line)

class ActivityError(Exception):
    def __init__(self, error, cause):
        super(ActivityError, self).__init__(cause)
        self.error = error
        self.cause = cause

