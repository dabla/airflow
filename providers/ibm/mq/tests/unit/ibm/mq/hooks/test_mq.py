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

import asyncio
import operator
import threading

from functools import reduce
from itertools import count
from unittest import mock
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from airflow.models import Connection
from airflow.providers.ibm.mq.hooks.mq import (
    _BACKOFF_BASE,
    _BACKOFF_FACTOR,
    _BACKOFF_MAX,
    IBMMQHook,
)

MQ_PAYLOAD = """RFH x"MQSTR    <mcd><Msd>jms_map</Msd></mcd>   <jms><Dst>topic://localhost/topic</Dst><Tms>1772121947476</Tms><Dlv>2</Dlv><Uci dt='bin.hex'>414D5143514D4941303054202020202069774D7092F81057</Uci></jms>L<usr><XMSC_CLIENT_ID>local</XMSC_CLIENT_ID><release>26.01.00</release></usr> 4<mqps><Top>topic</Top></mqps>  {}"""


def mq_connection():
    """Create a test MQ connection object."""
    return Connection(
        conn_id="mq_conn",
        conn_type="mq",
        host="mq.example.com",
        login="user",
        password="pass",
        port=1414,
        extra='{"queue_manager": "QM1", "channel": "DEV.APP.SVRCONN"}',
    )


@pytest.fixture
def mock_get_connection():
    """Fixture that mocks BaseHook.get_connection to return a test connection."""
    with patch("airflow.providers.ibm.mq.hooks.mq.BaseHook.get_connection") as mock_conn:
        mock_conn.return_value = mq_connection()
        yield mock_conn


@pytest.fixture
def patch_sync_to_async():
    """Patch sync_to_async to call the wrapped function directly for testing."""

    def sync_to_async(func):
        """Wrap a sync function so it can be awaited directly."""

        async def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        return wrapper

    with patch("airflow.providers.ibm.mq.hooks.mq.sync_to_async", side_effect=sync_to_async):
        yield


def fake_get(*args, **kwargs):
    import ibmmq

    raise ibmmq.MQMIError("connection broken", reason=ibmmq.CMQC.MQRC_CONNECTION_BROKEN)


@pytest.mark.asyncio
class TestIBMMQHook:
    """Tests for the IBM MQ hook."""

    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id="mq_conn",
                conn_type="mq",
                host="mq.example.com",
                login="user",
                password="pass",
                port=1414,
                extra='{"queue_manager": "QM1", "channel": "DEV.APP.SVRCONN"}',
            )
        )
        self.hook = IBMMQHook("mq_conn")

    @pytest.mark.parametrize(
        "open_options_attr",
        [
            "MQGMO_NO_WAIT",
            "MQOO_INPUT_EXCLUSIVE",
            "MQOO_INPUT_SHARED",
        ],
    )
    async def test_get_conn_with_open_options(self, mock_get_connection, open_options_attr):
        import ibmmq

        open_options = getattr(ibmmq.CMQC, open_options_attr)
        hook = IBMMQHook("mq_conn")

        assert not hook.open_options

        hook.open_options = open_options
        with hook.get_conn() as conn:
            assert hook.open_options == open_options

    @pytest.mark.parametrize(
        ("open_options_attr", "expected_flags"),
        [
            ("MQGMO_NO_WAIT", []),
            ("MQOO_INPUT_EXCLUSIVE", ['MQOO_INPUT_EXCLUSIVE']),
            ("MQOO_INPUT_SHARED", ['MQOO_INPUT_SHARED']),
            ("MQOO_INPUT_SHARED | MQOO_FAIL_IF_QUIESCING", ['MQOO_INPUT_EXCLUSIVE', 'MQOO_INPUT_SHARED', 'MQOO_FAIL_IF_QUIESCING'])
        ],
    )
    async def test_get_open_options_flags(self, mock_get_connection, open_options_attr, expected_flags):
        import ibmmq

        open_options = list(map(lambda open_option: getattr(ibmmq.CMQC, open_option), open_options_attr.split("|")))
        combined_options = reduce(operator.or_, open_options)
        flags = IBMMQHook.get_open_options_flags(combined_options)

        assert flags == expected_flags

    @patch("ibmmq.connect")
    @patch("ibmmq.Queue")
    async def test_aconsume_message(
        self, mock_queue_class, mock_connect, mock_get_connection, patch_sync_to_async
    ):
        """Test consuming a single message."""

        mock_qmgr = MagicMock()
        mock_connect.return_value = mock_qmgr

        mock_queue = MagicMock()
        mock_queue_class.return_value = mock_queue
        mock_queue.get.return_value = MQ_PAYLOAD.format("test message").encode()

        result = await self.hook.aconsume(queue_name="QUEUE1", poll_interval=0.1)
        assert isinstance(result, str)
        assert "test message" in result

        mock_connect.assert_called_once()
        mock_queue_class.assert_called_once_with(
            mock_qmgr,
            mock.ANY,
            mock.ANY,
        )

    @patch("ibmmq.connect")
    @patch("ibmmq.Queue")
    async def test_aproduce_message(
        self, mock_queue_class, mock_connect, mock_get_connection, patch_sync_to_async
    ):
        """Test producing a message to the queue."""

        mock_qmgr = MagicMock()
        mock_connect.return_value = mock_qmgr

        mock_queue = MagicMock()
        mock_queue_class.return_value = mock_queue

        await self.hook.aproduce(queue_name="QUEUE1", payload="payload")

        mock_connect.assert_called_once()
        mock_queue_class.assert_called_once_with(
            mock_qmgr,
            mock.ANY,
            mock.ANY,
        )
        mock_queue.put.assert_called_once()

    @patch("ibmmq.connect")
    @patch("ibmmq.Queue")
    async def test_aconsume_connection_broken(
        self, mock_queue_class, mock_connect, mock_get_connection, patch_sync_to_async, caplog
    ):
        """Test that consume logs a warning on connection broken."""

        mock_qmgr = MagicMock()
        mock_connect.return_value = mock_qmgr
        mock_queue = MagicMock()
        mock_queue_class.return_value = mock_queue
        mock_queue.get.side_effect = fake_get

        # aconsume() retries on None, so we need to cancel after the first attempt
        with patch("airflow.providers.ibm.mq.hooks.mq.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            mock_sleep.side_effect = asyncio.CancelledError
            with pytest.raises(asyncio.CancelledError):
                await self.hook.aconsume(queue_name="QUEUE1", poll_interval=0.1)

        assert "MQ connection broken on queue 'QUEUE1'; will reconnect" in caplog.text

    @patch("airflow.providers.ibm.mq.hooks.mq.asyncio.sleep", new_callable=AsyncMock)
    async def test_aconsume_retries_on_none_then_succeeds(self, mock_sleep, patch_sync_to_async):
        counter = count()

        def consume(
            queue_name: str,
            poll_interval: float,
            stop_event: threading.Event,
        ) -> str | None:
            assert queue_name == "QUEUE1"
            assert poll_interval == 0.1

            if stop_event.is_set():
                raise RuntimeError("Should not occur in this test!")

            if next(counter) < 2:
                return None
            return "payload after retries"

        """When consume returns None, aconsume retries with backoff until a message arrives."""
        with patch.object(self.hook, "consume", side_effect=consume):
            result = await self.hook.aconsume(queue_name="QUEUE1", poll_interval=0.1)

        assert result == "payload after retries"
        assert mock_sleep.call_count == 2
        mock_sleep.assert_any_call(_BACKOFF_BASE)
        mock_sleep.assert_any_call(_BACKOFF_BASE * _BACKOFF_FACTOR)

    @patch("airflow.providers.ibm.mq.hooks.mq.asyncio.sleep", new_callable=AsyncMock)
    async def test_aconsume_retries_on_exception_then_succeeds(self, mock_sleep, patch_sync_to_async):
        """When consume raises, aconsume retries with backoff."""
        with patch.object(self.hook, "consume", side_effect=[ConnectionError("broken"), "recovered"]):
            result = await self.hook.aconsume(queue_name="QUEUE1", poll_interval=0.1)

        assert result == "recovered"
        mock_sleep.assert_called_once_with(_BACKOFF_BASE)

    @patch("airflow.providers.ibm.mq.hooks.mq.asyncio.sleep", new_callable=AsyncMock)
    async def test_aconsume_backoff_caps_at_max(self, mock_sleep, patch_sync_to_async):
        """Backoff delay should not exceed _BACKOFF_MAX."""
        failures_needed = 0
        backoff = _BACKOFF_BASE
        while backoff < _BACKOFF_MAX:
            backoff *= _BACKOFF_FACTOR
            failures_needed += 1
        failures_needed += 3

        with patch.object(self.hook, "consume", side_effect=[None] * failures_needed + ["finally"]):
            result = await self.hook.aconsume(queue_name="QUEUE1", poll_interval=0.1)

        assert result == "finally"
        capped_calls = [c for c in mock_sleep.call_args_list if c.args[0] == _BACKOFF_MAX]
        assert len(capped_calls) >= 3

    @patch("airflow.providers.ibm.mq.hooks.mq.asyncio.sleep", new_callable=AsyncMock)
    async def test_aconsume_logs_warning_on_none(self, mock_sleep, patch_sync_to_async, caplog):
        """A warning is logged when consume returns None."""
        with patch.object(self.hook, "consume", side_effect=[None, "message"]):
            with caplog.at_level("WARNING"):
                await self.hook.aconsume(queue_name="QUEUE1", poll_interval=0.1)

        assert "IBM MQ consume returned no event" in caplog.text

    @patch("airflow.providers.ibm.mq.hooks.mq.asyncio.sleep", new_callable=AsyncMock)
    async def test_aconsume_logs_warning_on_exception(self, mock_sleep, patch_sync_to_async, caplog):
        """A warning with traceback is logged when consume raises."""
        with patch.object(self.hook, "consume", side_effect=[RuntimeError("boom"), "ok"]):
            with caplog.at_level("WARNING"):
                await self.hook.aconsume(queue_name="QUEUE1", poll_interval=0.1)

        assert "IBM MQ consume encountered an error" in caplog.text

    @patch("airflow.providers.ibm.mq.hooks.mq.asyncio.sleep", new_callable=AsyncMock)
    async def test_aconsume_cancelled_error_propagates(self, mock_sleep, patch_sync_to_async):
        """CancelledError during backoff sleep propagates out of consume."""
        mock_sleep.side_effect = asyncio.CancelledError

        with patch.object(self.hook, "consume", return_value=None):
            with pytest.raises(asyncio.CancelledError):
                await self.hook.aconsume(queue_name="QUEUE1", poll_interval=0.1)

    @patch("ibmmq.connect")
    @patch("ibmmq.Queue")
    @patch("airflow.providers.ibm.mq.hooks.mq.sync_to_async")
    async def test_aconsume_propagates_non_mq_exceptions(
        self, mock_sync_to_async, mock_queue_class, mock_connect, mock_get_connection
    ):
        """Non-MQ exceptions (e.g. TypeError) in consume propagate instead of being swallowed."""
        import threading

        mock_qmgr = MagicMock()
        mock_connect.return_value = mock_qmgr
        mock_queue = MagicMock()
        mock_queue_class.return_value = mock_queue
        mock_queue.get.side_effect = TypeError("Unexpected programming bug")

        stop_event = threading.Event()
        with pytest.raises(TypeError, match="Unexpected programming bug"):
            self.hook.consume("QUEUE1", 0.1, stop_event)
