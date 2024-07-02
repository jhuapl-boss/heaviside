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

import json
import sys
from io import IOBase, StringIO
from contextlib import contextmanager

from boto3.session import Session

# With Python3.11 Mapping is imported from collections.abc
# Try to import with the new method and if it fails fall back to old way for compatibility
try:
    from collections.abc import Mapping
except ImportError:
    from collections import Mapping


try:
    from urllib.request import urlopen
except ImportError:
    from urllib2 import urlopen

try:
    from pathlib import Path
except ImportError:
    import os
    import io

    class Path(object):
        """Stub Path object that implements only the features used by heaviside"""
        def __init__(self, path = '/'):
            self.path = path

        def open(self, *args, **kwargs):
            return io.open(self.path, *args, **kwargs)

        def __div__(self, path):
            return Path(os.path.join(self.path, path))

def isstr(obj):
    """Determine if the given object is a string

    This function support comparing against the Python2 <unicode> type as well
    as the Python2/3 <str> type.

    Args:
        obj (object) : Object to check

    Returns:
        bool : If the object is a str or unicode type
    """
    try: # Python 2 compatibility
        is_unicode = isinstance(obj, unicode)
    except NameError:
        is_unicode = False

    return isinstance(obj, str) or is_unicode

@contextmanager
def read(obj):
    """Context manager for reading data from multiple sources as a file object

    Args:
        obj (string|Path|file object): Data to read / read from
                                  If obj is a file object, this is just a pass through
                                  If obj is a Path object, this is similar to obj.open()
                                  If obj is a string, this creates a StringIO so
                                     the data can be read like a file object

    Returns:
        file object: File handle containing data
    """
    is_open = False
    if isinstance(obj, Path):
        fh = obj.open()
        is_open = True
    elif isstr(obj):
        fh = StringIO(obj)
        fh.name = '<string>'
    elif isinstance(obj, IOBase):
        fh = obj
    else:
        raise ValueError("Unknown input type {}".format(type(obj).__name__))
    
    try:
        yield fh
    finally:
        if is_open:
            fh.close()

@contextmanager
def write(obj):
    """Context manager for writing data to multiple sources as a file object

    Args:
        obj (string|Path|file object): Data to read / read from
                                  If obj is a file object, this is just a pass through
                                  If obj is a Path object, this is similar to obj.open()
                                  If obj is a string, and is '-' then sys.stdout is used
                                     else this is similar to Path(obj).open()

    Returns:
        file object: File handle ready to write data
    """
    fh = None
    is_open = False
    if isstr(obj):
        if obj == '-':
            fh = sys.stdout
        else:
            obj = Path(obj)

    if fh is None:
        if isinstance(obj, Path):
            fh = obj.open('w')
            is_open = True
        elif isinstance(obj, IOBase):
            fh = obj
        else:
            raise ValueError("Unknown input type {}".format(type(obj).__name__))

    try:
        yield fh
    finally:
        if is_open:
            fh.close()

def create_session(**kwargs):
    """Create a Boto3 session from multiple different sources

    Basic file format / dictionary format:
    {
        'aws_secret_key': '',
        'aws_access_key': '',
        'aws_region': '',
        'aws_account_id': ''
    }

    Note: If no arguments are given, a Boto3 session is created and it will attempt
          to figure out this information for itself, from predefined locations.

    Args:
        session (Boto3 Session): Existing session to use. Only account id will be looked for
        credentials (dict|fh|Path|json string): source to load credentials from
                                                If a dict, used directly
                                                If a fh, read and parsed as a Json object
                                                If a Path, opened, read, and parsed as a Json object
                                                If a string, parsed as a Json object

        Note: The following will override the values in credentials if they exist
        secret_key / aws_secret_key (string): AWS Secret Key
        access_key / aws_access_key (string): AWS Access Key

        Note: The following will be derived from the AWS Session if not provided
        account_id / aws_account_id (string): AWS Account ID

        Note: The following will be pulled from EC2 Meta-data if not provided
        region / aws_region (string): AWS region to connect to

    Returns:
        (Boto3 Session, account_id) : Boto3 session populated with given credentials and
                                      AWS Account ID (either given or derived from session)
    """
    credentials = kwargs.get('credentials', {})
    if isinstance(credentials, Mapping):
        pass
    elif isinstance(credentials, Path):
        with credentials.open() as fh:
            credentials = json.load(fh)
    elif isinstance(credentials, str):
        credentials = json.loads(credentials)
    elif isinstance(credentials, IOBase):
        credentials = json.load(credentials)
    else:
        raise Exception("Unknown credentials type: {}".format(type(credentials).__name__))

    def locate(names, locations):
        for location in locations:
            for name in names:
                if name in location:
                    return location[name]
        names = " or ".join(names)
        raise Exception("Could not find credentials value for {}".format(names))

    locate_region = True
    if 'session' in kwargs:
        session = kwargs['session']
        locate_region = False
    else:
        try:
            access = locate(('access_key', 'aws_access_key'), (kwargs, credentials))
            secret = locate(('secret_key', 'aws_secret_key'), (kwargs, credentials))

            session = Session(aws_access_key_id = access,
                              aws_secret_access_key = secret)
        except:
            session = Session() # Let boto3 try to resolve the keys iteself, potentially from EC2 meta data

    try:
        account_id = locate(('account_id', 'aws_account_id'), (kwargs, credentials))
    except:
        # From boss-manage.git/lib/aws.py:get_account_id_from_session()
        account_id = session.client('iam').list_users(MaxItems=1)["Users"][0]["Arn"].split(':')[4]

    if locate_region:
        try:
            region = locate(('region', 'aws_region'), (kwargs, credentials))
        except:
            try:
                region = urlopen('http://169.254.169.254/latest/meta-data/placement/availability-zone/')
                region = region.read().decode('utf-8')[:-1] # remove AZ character from region name
            except:
                raise Exception("Could not locate AWS region to connect to")

        # DP HACK: No good way to get the region after the session is created
        #          Needed here to support loading keys from EC2 meta data
        session._session.set_config_variable('region', region)

    return session, account_id

