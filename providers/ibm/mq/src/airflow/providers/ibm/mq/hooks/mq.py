#
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
import json
import threading
from contextlib import contextmanager, suppress
from typing import Any

from asgiref.sync import sync_to_async

from airflow.providers.common.compat.sdk import BaseHook

# Backoff parameters for transient consume failures
_BACKOFF_BASE: float = 1.0
_BACKOFF_MAX: float = 60.0
_BACKOFF_FACTOR: float = 2.0


class IBMMQHook(BaseHook):
    """
    Interact with IBM MQ queue managers to consume and produce messages.

    This hook wraps the ``ibmmq`` C client and manages connection
    lifecycle, queue open/close, and message serialization.  Both synchronous
    (context-manager) and asynchronous (``consume`` / ``produce``) interfaces
    are provided.

    Connection parameters (host, port, login, password) are read from the
    Airflow connection identified by *conn_id*.  ``queue_manager``, ``channel``,
    and ``open_options`` can be supplied either as constructor arguments or via
    the connection's *extra* JSON — constructor arguments take precedence.

    :param conn_id: Airflow connection ID for the IBM MQ instance.
        Defaults to ``"mq_default"``.
    :param queue_manager: Name of the IBM MQ queue manager to connect to.
        If not provided, the value is read from the ``queue_manager`` key in
        the connection's *extra* JSON.
    :param channel: MQ channel name used for the connection.
        If not provided, the value is read from the ``channel`` key in the
        connection's *extra* JSON.
    :param open_options: Integer bitmask of ``MQOO_*`` open options passed
        when opening a queue (e.g.,
        ``ibmmq.CMQC.MQOO_INPUT_SHARED | ibmmq.CMQC.MQOO_FAIL_IF_QUIESCING``).
        If not provided, the value is resolved from the ``open_options`` key
        in the connection's *extra* JSON, falling back to
        ``MQOO_INPUT_SHARED``.
    """

    conn_name_attr = "conn_id"
    default_conn_name = "mq_default"
    conn_type = "ibmmq"
    hook_name = "IBM MQ"
    default_open_options = "MQOO_INPUT_SHARED"

    def __init__(
        self,
        conn_id: str = default_conn_name,
        queue_manager: str | None = None,
        channel: str | None = None,
        open_options: int | None = None,
    ):
        super().__init__()
        self.conn_id = conn_id
        self.queue_manager = queue_manager
        self.channel = channel
        self.open_options = open_options
        self._conn = None

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom UI field behaviour for IBM MQ Connection."""
        return {
            "hidden_fields": ["schema"],
            "placeholders": {
                "host": "mq.example.com",
                "port": "1414",
                "login": "app_user",
                "extra": json.dumps(
                    {
                        "queue_manager": "QM1",
                        "channel": "DEV.APP.SVRCONN",
                        "open_options": cls.default_open_options,
                    },
                    indent=2,
                ),
            },
        }

    @classmethod
    def get_open_options_flags(cls, open_options: int) -> list[str]:
        """
        Return the symbolic MQ open option flags set in a given bitmask.

        Each flag corresponds to a constant in ``ibmmq.CMQC`` that starts with ``MQOO_``.

        :param open_options: The integer bitmask used when opening an MQ queue
                             (e.g., ``MQOO_INPUT_EXCLUSIVE | MQOO_FAIL_IF_QUIESCING``).

        :return: A list of the names of the MQ open flags that are set in the bitmask.
                 For example, ``['MQOO_INPUT_EXCLUSIVE', 'MQOO_FAIL_IF_QUIESCING']``.

        Example:
            >>> open_options = ibmmq.CMQC.MQOO_INPUT_SHARED | ibmmq.CMQC.MQOO_FAIL_IF_QUIESCING
            >>> cls.get_open_options_flags(open_options)
            ['MQOO_INPUT_SHARED', 'MQOO_FAIL_IF_QUIESCING']
        """
        import ibmmq

        return [
            name
            for name, value in vars(ibmmq.CMQC).items()
            if name.startswith("MQOO_") and (open_options & value)
        ]

    @staticmethod
    def _connect(queue_manager: str, channel: str, conn_info: str, csp):
        """
        Connect to the IBM MQ queue manager.

        Connection errors from the C client are caught and re-raised as a
        :class:`ConnectionError` with a human-readable message.

        :return: IBM MQ connection object
        """
        import ibmmq

        try:
            return ibmmq.connect(queue_manager, channel, conn_info, csp=csp)
        except (ibmmq.MQMIError, ibmmq.PYIFError) as e:
            raise ConnectionError(
                f"Failed to connect to IBM MQ queue manager '{queue_manager}' "
                f"at {conn_info} on channel '{channel}': {e}"
            ) from e

    @contextmanager
    def get_conn(self):
        """
        Sync context manager for IBM MQ connection lifecycle.

        Must be called from the executor thread (not the event loop thread).
        Retrieves the Airflow connection, extracts MQ parameters, and manages
        the IBM MQ connection lifecycle.

        :yield: IBM MQ connection object
        """
        import ibmmq

        connection = BaseHook.get_connection(self.conn_id)
        config = connection.extra_dejson
        queue_manager = self.queue_manager or config.get("queue_manager")
        channel = self.channel or config.get("channel")

        if not queue_manager:
            raise ValueError("queue_manager must be set in Connection extra config or hook init")
        if not channel:
            raise ValueError("channel must be set in Connection extra config or hook init")

        if self.open_options is None:
            self.open_options = getattr(
                ibmmq.CMQC,
                config.get("open_options", self.default_open_options),
                ibmmq.CMQC.MQOO_INPUT_SHARED,
            )

        csp = ibmmq.CSP()
        csp.CSPUserId = connection.login
        csp.CSPPassword = connection.password

        conn_info = f"{connection.host}({connection.port})"
        conn = self._connect(queue_manager, channel, conn_info, csp)
        try:
            yield conn
        finally:
            with suppress(Exception):
                conn.disconnect()

    def _process_message(self, message: bytes) -> str:
        """
        Process a raw MQ message.

        If the message contains an RFH2 header, the header is unpacked and the
        payload following the header is returned. If unpacking fails, the raw
        message is returned decoded as UTF-8.

        :param message: Raw message received from IBM MQ.
        :return: Decoded message payload.
        """
        import ibmmq

        try:
            rfh2 = ibmmq.RFH2()
            rfh2.unpack(message)

            payload_offset = rfh2.get_length()
            payload = message[payload_offset:]

            decoded = payload.decode("utf-8", errors="ignore")
            self.log.debug("Message received from MQ (RFH2 decoded): %s", decoded)
            return decoded
        except ibmmq.PYIFError as error:  # RFH2 header not present or unpack failed
            self.log.warning(
                "Failed to unpack RFH2 header (%s). Returning raw message payload: %s", error, message
            )
            return message.decode("utf-8", errors="ignore")

    async def aconsume(self, queue_name: str, poll_interval: float = 5) -> str:
        """
        Asynchronous version of :meth:`consume`.

        Wait for a single message from the specified IBM MQ queue and return its decoded payload.

        The method retries with exponential back-off whenever the underlying
        :meth:`consume` returns ``None`` (connection broken, timeout) or raises
        an unexpected exception, so that an AssetWatcher is never silently killed
        by a transient failure.

        All blocking IBM MQ operations ('connect', 'open', 'get', 'close', 'disconnect') run in a
        separate thread via 'sync_to_async' to satisfy the IBM MQ C client's thread-affinity
        requirement — every operation on a connection must happen from the thread that created it.

        A :class:`threading.Event` stop signal is passed to the worker so that, when this
        coroutine is canceled (e.g. because the Airflow triggerer reassigns the watcher to
        another pod), the background thread exits cleanly after the current 'q.get' call
        times out (at most 'poll_interval' seconds).  Without this, an orphaned thread could
        silently consume a message after cancellation, causing the event to be lost and the
        DAG never to run.

        :param queue_name: Name of the IBM MQ queue to consume messages from.
        :param poll_interval: Interval in seconds used to wait for messages and to control
            how long the underlying MQ 'get' operation blocks before checking again.
        :return: The decoded message payload.
        """
        import ibmmq

        backoff = _BACKOFF_BASE
        while True:
            stop_event = threading.Event()
            try:
                result = await sync_to_async(self.consume)(queue_name, poll_interval, stop_event)
            except asyncio.CancelledError:
                stop_event.set()
                raise
            except ibmmq.MQMIError as e:
                if e.reason == ibmmq.CMQC.MQRC_CONNECTION_BROKEN:
                    self.log.warning(
                        "Transient MQ error on queue '%s': completion_code=%s reason_code=%s (%s); retrying in %.1fs",
                        queue_name, e.comp, e.reason, e, backoff,
                    )
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * _BACKOFF_FACTOR, _BACKOFF_MAX)
                    continue
                self.log.error(
                    "Permanent MQ error on queue '%s': completion_code=%s reason_code=%s (%s) -- not retrying",
                    queue_name, e.comp, e.reason, e,
                )
                raise
            except ibmmq.PYIFError as e:
                self.log.error(
                    "PYIFError on queue '%s': %s -- not retrying",
                    queue_name, e,
                )
                raise
            except Exception as e:
                # Programming errors should not be retried
                self.log.error(
                    "Unexpected error in IBM MQ consume for queue '%s': %s -- not retrying",
                    queue_name, e,
                    exc_info=True,
                )
                raise

            if result is not None:
                stop_event.set()
                return result

            # Only DEBUG log for quiet queue (no message available)
            self.log.debug(
                "IBM MQ consume returned no event for queue '%s'; queue may be quiet. Retrying in %.1fs",
                queue_name,
                backoff,
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * _BACKOFF_FACTOR, _BACKOFF_MAX)

    def consume(
        self,
        queue_name: str,
        poll_interval: float,
        stop_event: threading.Event,
    ) -> str | None:
        """
        Blocking implementation that consumes a single message from the given IBM MQ queue.

        All IBM MQ handles (queue manager connection, queue) are created **and used** within
        this method, satisfying the thread-affinity requirement of the IBM MQ C client library.
        The 'stop_event' is checked between 'q.get' calls so the thread terminates promptly
        after the coroutine side is canceled.

        For an asynchronous interface see :meth:`aconsume`.
        """
        import ibmmq

        od = ibmmq.OD()
        od.ObjectName = queue_name

        md = ibmmq.MD()
        md.Format = ibmmq.CMQC.MQFMT_STRING
        md.CodedCharSetId = 1208
        md.Encoding = ibmmq.CMQC.MQENC_NATIVE

        gmo = ibmmq.GMO()
        gmo.Options = ibmmq.CMQC.MQGMO_WAIT | ibmmq.CMQC.MQGMO_NO_SYNCPOINT | ibmmq.CMQC.MQGMO_CONVERT
        gmo.WaitInterval = int(poll_interval * 1000)

        with self.get_conn() as conn:
            if self.open_options is not None:
                flag_names = self.get_open_options_flags(self.open_options)
                self.log.info(
                    "Opening MQ queue '%s' with open_options=%s (%s)",
                    queue_name,
                    self.open_options,
                    ", ".join(flag_names),
                )

            q = ibmmq.Queue(conn, od, self.open_options)
            try:
                # WaitInterval already blocks for poll_interval seconds when no message is
                # available, so no additional sleep is needed between iterations.
                while not stop_event.is_set():
                    try:
                        message = q.get(None, md, gmo)
                        if message:
                            return self._process_message(message)
                    except ibmmq.MQMIError as e:
                        if e.reason == ibmmq.CMQC.MQRC_NO_MSG_AVAILABLE:
                            self.log.debug("No message available on queue '%s' (reason=%s)", queue_name, e.reason)
                            continue
                        # For all other MQMIError, log and re-raise immediately
                        self.log.error(
                            "IBM MQ error on queue '%s': completion_code=%s reason_code=%s (%s)",
                            queue_name, e.comp, e.reason, e,
                        )
                        raise
            finally:
                with suppress(Exception):
                    q.close()
        return None

    async def aproduce(self, queue_name: str, payload: str) -> None:
        """
        Asynchronous version of :meth:`produce`.

        Put a message onto the specified IBM MQ queue.

        All blocking IBM MQ operations run in a separate thread via 'sync_to_async' for the same
        thread-safety reasons as :meth:`aconsume`.

        :param queue_name: Name of the IBM MQ queue to which the message should be sent.
        :param payload: Message payload to send. The payload will be encoded as UTF-8
            before being placed on the queue.
        :return: None
        """
        await sync_to_async(self.produce)(queue_name, payload)

    def produce(
        self,
        queue_name: str,
        payload: str,
    ) -> None:
        """Blocking implementation of :meth:`aproduce`."""
        import ibmmq

        od = ibmmq.OD()
        od.ObjectName = queue_name

        md = ibmmq.MD()
        md.Format = ibmmq.CMQC.MQFMT_STRING
        md.CodedCharSetId = 1208
        md.Encoding = ibmmq.CMQC.MQENC_NATIVE

        try:
            with self.get_conn() as conn:
                q = ibmmq.Queue(conn, od, ibmmq.CMQC.MQOO_OUTPUT)
                try:
                    q.put(payload.encode("utf-8"), md)
                finally:
                    with suppress(Exception):
                        q.close()
        except Exception:
            self.log.exception(
                "MQ produce failed on queue '%s'",
                queue_name,
            )
            raise
