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


try:
    from unittest import mock
except ImportError:
    import mock

try:
    from pathlib import PosixPath
except ImportError:
    from heaviside.utils import Path as PosixPath

class MockPath(PosixPath):
    def __init__(self, path):
        super(MockPath, self).__init__()
        self.open = mock.MagicMock()
        # close() is done on the return of open()

class MockSession(object):
    """Mock Boto3 Session object that creates unique mock objects
    for each client type requested"""

    def __init__(self, **kwargs):
        super(MockSession, self).__init__()

        self.clients = {}
        self.kwargs = kwargs
        self.region_name = "region"

    def client(self, name, config=None):
        """Create a new client session

        Args:
            name (string): Name of the client to connect to

        Returns:
            MagicMock: The unique MagicMock object for the requested client
        """
        if name not in self.clients:
            self.clients[name] = mock.MagicMock()
        return self.clients[name]

    @property
    def _session(self):
        return self

    def set_config_variable(self, name, value):
        self.__dict__[name] = value
