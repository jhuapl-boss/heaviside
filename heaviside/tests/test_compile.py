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

import os
import sys
import unittest
from io import StringIO

try:
    from unittest import mock
except ImportError:
    import mock

import heaviside
Path = heaviside.utils.Path

cur_dir = Path(os.path.dirname(os.path.realpath(__file__)))

class TestCompile(unittest.TestCase):
    def execute(self, filename, error_msg):
        filepath = cur_dir / 'sfn' / filename
        stderr = StringIO()

        out = heaviside.compile(filepath, file=stderr)

        stderr.seek(0)
        actual = stderr.read().split('\n')[3]
        expected = "Syntax Error: {}".format(error_msg)

        self.assertEqual(out, None)
        self.assertEqual(actual, expected)

    def test_unterminated_quote(self):
        self.execute('error_unterminated_quote.sfn', 'Unterminated quote')

    def test_unterminated_multiquote(self):
        self.execute('error_unterminated_multiquote.sfn', 'EOF in multi-line string')

    def test_invalid_heartbeat(self):
        self.execute('error_invalid_heartbeat.sfn', "Modifier 'heartbeat' must be less than 'timeout'")

    def test_unexpected_catch(self):
        self.execute('error_unexpected_catch.sfn', "Invalid modifier 'catch'")

    def test_unexpected_data(self):
        self.execute('error_unexpected_data.sfn', "Invalid modifier 'data'")

    def test_unexpected_heartbeat(self):
        self.execute('error_unexpected_heartbeat.sfn', "Invalid modifier 'heartbeat'")

    def test_unexpected_input(self):
        self.execute('error_unexpected_input.sfn', "Invalid modifier 'input'")

    def test_unexpected_output(self):
        self.execute('error_unexpected_output.sfn', "Invalid modifier 'output'")

    def test_unexpected_result(self):
        self.execute('error_unexpected_result.sfn', "Invalid modifier 'result'")

    def test_unexpected_retry(self):
        self.execute('error_unexpected_retry.sfn', "Invalid modifier 'retry'")

    def test_unexpected_timeout(self):
        self.execute('error_unexpected_timeout.sfn', "Invalid modifier 'timeout'")

    def test_unexpected_token(self):
        self.execute('error_unexpected_token.sfn', 'Invalid syntax')

