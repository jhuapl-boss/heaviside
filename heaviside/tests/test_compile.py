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
import json
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

        try:
            out = heaviside.compile(filepath)
            self.assertFalse(True, "compile() should result in an exception")
        except heaviside.exceptions.CompileError as ex:
            actual = str(ex).split('\n')[-1]
            expected = "Syntax Error: {}".format(error_msg)
            self.assertEqual(actual, expected)

    def test_unterminated_quote(self):
        self.execute('error_unterminated_quote.sfn', 'Unterminated quote')

    def test_unterminated_multiquote(self):
        self.execute('error_unterminated_multiquote.sfn', 'EOF in multi-line string')

    def test_invalid_heartbeat(self):
        self.execute('error_invalid_heartbeat.sfn', "Heartbeat must be less than timeout (defaults to 60)")

    def test_invalid_heartbeat2(self):
        self.execute('error_invalid_heartbeat2.sfn', "'0' is not a positive integer")

    def test_invalid_timeout(self):
        self.execute('error_invalid_timeout.sfn', "'0' is not a positive integer")

    def test_unexpected_catch(self):
        self.execute('error_unexpected_catch.sfn', "Pass state cannot contain a Catch modifier")

    def test_unexpected_data(self):
        self.execute('error_unexpected_data.sfn', "Succeed state cannot contain a Data modifier")

    def test_unexpected_heartbeat(self):
        self.execute('error_unexpected_heartbeat.sfn', "Pass state cannot contain a Heartbeat modifier")

    def test_unexpected_input(self):
        self.execute('error_unexpected_input.sfn', "Fail state cannot contain a Input modifier")

    def test_unexpected_output(self):
        self.execute('error_unexpected_output.sfn', "Fail state cannot contain a Output modifier")

    def test_unexpected_result(self):
        self.execute('error_unexpected_result.sfn', "Fail state cannot contain a Result modifier")

    def test_unexpected_retry(self):
        self.execute('error_unexpected_retry.sfn', "Pass state cannot contain a Retry modifier")

    def test_unexpected_timeout(self):
        self.execute('error_unexpected_timeout.sfn', "Pass state cannot contain a Timeout modifier")

    def test_unexpected_token(self):
        self.execute('error_unexpected_token.sfn', 'Invalid syntax')

    def test_invalid_retry_delay(self):
        self.execute('error_invalid_retry_delay.sfn', "'0' is not a positive integer")

    def test_invalid_retry_backoff(self):
        self.execute('error_invalid_retry_backoff.sfn', "Backoff rate should be >= 1.0")

    def test_invalid_wait_seconds(self):
        self.execute('error_invalid_wait_seconds.sfn', "'0' is not a positive integer")

    def test_invalid_multiple_input(self):
        self.execute('error_invalid_multiple_input.sfn', "Pass state can only contain one Input modifier")

    def test_invalid_state_name(self):
        self.execute('error_invalid_state_name.sfn', "Name exceedes 128 characters")

    def test_duplicate_state_name(self):
        self.execute('error_duplicate_state_name.sfn', "Duplicate state name 'Test'")

    def test_invalid_goto_target(self):
        self.execute('error_invalid_goto_target.sfn', "Goto target 'Target' doesn't exist")

    def test_invalid_task_service(self):
        self.execute('error_invalid_task_service.sfn', "Invalid Task service")

    def test_missing_task_function_argument(self):
        self.execute('error_missing_task_function_argument.sfn', "Lambda task requires a function name argument")

    def test_missing_task_function(self):
        self.execute('error_missing_task_function.sfn', "DynamoDB task requires a function to call")

    def test_unexpected_task_function(self):
        self.execute('error_unexpected_task_function.sfn', "Unexpected function name")

    def test_invalid_task_function(self):
        self.execute('error_invalid_task_function.sfn', "Invalid Task function")

    def test_invalid_task_arn(self):
        self.execute('error_invalid_task_arn.sfn', "ARN must start with 'arn:aws:'")

    def test_unexpected_task_argument(self):
        self.execute('error_unexpected_task_argument.sfn', "Unexpected argument")

    def test_unexpected_task_keyword_argument(self):
        self.execute('error_unexpected_task_keyword_argument.sfn', "Unexpected keyword argument")

    def test_invalid_task_sync_value(self):
        self.execute('error_invalid_task_sync_value.sfn', "Synchronous value must be a boolean")

    def test_invalid_task_keyword_argument(self):
        self.execute('error_invalid_task_keyword_argument.sfn', "Invalid keyword argument")

    def test_missing_task_keyword_argument(self):
        self.execute('error_missing_task_keyword_argument.sfn', "Missing required keyword arguments: JobDefinition, JobQueue")

    def test_visitor(self):
        class TestVisitor(heaviside.ast.StateVisitor):
            def handle_task(self, task):
                task.arn = 'modified'

        hsd = u"""Lambda('function')"""
        out = heaviside.compile(hsd, visitors=[TestVisitor()])
        out = json.loads(out)

        self.assertEqual(out['States']['Line1']['Resource'], 'modified')

    def test_invalid_iterator(self):
        self.execute('error_iterator_used_by_non_map_state.sfn', "Pass state cannot contain a Iterator modifier")

    def test_map_without_iterator(self):
        self.execute('error_map_has_no_iterator.sfn', 'Map state must have an iterator')

    def test_map_iterator_has_duplicate_state_name(self):
        self.execute('error_map_iterator_duplicate_state_name.sfn', "Duplicate state name 'DuplicateName'")
