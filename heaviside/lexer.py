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

import tokenize
import token

from .exceptions import CompileError

USELESS = ['NEWLINE', 'NL', 'COMMENT']

# DP TODO: Use __slots__ and or a generator to reduce overhead for tokenizing large files
class Token(object):
    """Custom class wrapping all token properties"""

    def __init__(self, code, value, start=(0,0), stop=(0,0), line=''):
        """
        Args:
            code (string|int): Token code. Ints are translated using token.tok_name.
            value (string): Token value
            start (tuple): Pair of values describing token start line, start position
            stop (tuple): Pair of values describing token stop line, stop position
            line (string): String containing the line the token was parsed from
        """
        try:
            self.code = token.tok_name[code]
        except:
            self.code = code
        self.value = value
        self.start = start
        self.stop = stop
        self.line = line

    def __str__(self):
        sl, sp = self.start
        el, ep = self.stop
        pos = '{},{}-{},{}'.format(sl, sp, el, ep)
        return '{}, {}, {!r}'.format(pos, self.code, self.value)

    def __repr__(self):
        return '{}({!r}, {!r}, {!r}, {!r}, {!r})'.format(self.__class__.__name__,
                                               self.code,
                                               self.value,
                                               self.start,
                                               self.stop,
                                               self.line)

    def __eq__(self, other):
        return (self.code, self.value) == (other.code, other.value)

def tokenize_source(source):
    """Tokenize a source and convert into Token objects

    Results are filtered by through the list of USELESS tokens

    Args:
        source (callable): Callable object which provides the same interface as io.IOBase.readline
                           Each call provides a line of input as bytes

    Returns:
        list: List of parsed tokens
    """
    try:
        tokens = tokenize.generate_tokens(source)
        tokens = [Token(*t) for t in tokens]
        return [t for t in tokens if t.code not in USELESS]
    except tokenize.TokenError as e:
        raise CompileError.from_tokenize(e)

