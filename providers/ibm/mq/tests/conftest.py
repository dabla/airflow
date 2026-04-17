# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import importlib.util
import sys
from types import ModuleType
from unittest.mock import MagicMock

import pytest

pytest_plugins = "tests_common.pytest_plugin"


@pytest.fixture(autouse=True)
def _ensure_fake_ibmmq_if_missing():
    """Autouse fixture that injects a fake `ibmmq` module into sys.modules
    for the duration of tests in this package when the real package is not
    available. This avoids global import-time mutations while preserving test
    behavior.
    """
    if importlib.util.find_spec("ibmmq") is not None:
        # Real package available, nothing to do.
        yield
        return

    # Create a minimal fake module with the attributes our tests expect.
    fake = ModuleType("ibmmq")

    class MQMIError(Exception):
        def __init__(self, msg="", reason=None):
            super().__init__(msg)
            self.reason = reason

    class PYIFError(Exception):
        def __init__(self, msg="", reason=None):
            super().__init__(msg)
            self.reason = reason

    fake.CMQC = MagicMock()
    fake.CMQC.MQRC_NO_MSG_AVAILABLE = 2033
    fake.CMQC.MQRC_CONNECTION_BROKEN = 2009
    fake.CMQC.MQGMO_WAIT = 1
    fake.CMQC.MQGMO_NO_SYNCPOINT = 2
    fake.CMQC.MQGMO_CONVERT = 4
    fake.MQMIError = MQMIError
    fake.PYIFError = PYIFError
    fake.OD = MagicMock()
    fake.MD = MagicMock()
    fake.GMO = MagicMock()
    fake.CSP = MagicMock()
    fake.Queue = MagicMock()
    fake.connect = MagicMock()
    fake.RFH2 = MagicMock()

    sys.modules["ibmmq"] = fake
    try:
        yield
    finally:
        # Remove the injected fake to avoid leaking into other tests
        sys.modules.pop("ibmmq", None)
