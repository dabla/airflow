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

import pytest

from airflow.providers.sftp.pools.sftp import SFTPClientPool


class TestSFTPClientPool:
    @pytest.fixture(autouse=True)
    def cleanup_singleton(self):
        """Clear SFTPClientPool._instances before and after each test to ensure test isolation."""
        # Clear before test
        SFTPClientPool._instances.clear()
        yield
        # Clear after test
        SFTPClientPool._instances.clear()

    @pytest.mark.asyncio
    async def test_acquire_and_release(self, sftp_hook_mocked):
        async with SFTPClientPool("test_conn", pool_size=2) as pool:
            ssh, sftp = await pool.acquire()
            assert ssh is not None
            assert sftp is not None

            await pool.release((ssh, sftp))
            ssh2, sftp2 = await pool.acquire()
            assert ssh2 is not None
            assert sftp2 is not None

    @pytest.mark.asyncio
    async def test_get_sftp_client_context_manager(self, sftp_hook_mocked):
        async with SFTPClientPool("test_conn", pool_size=1) as pool:
            async with pool.get_sftp_client() as sftp:
                assert sftp is not None

            # If the context manager releases correctly, the single slot can be acquired again.
            ssh2, sftp2 = await asyncio.wait_for(pool.acquire(), timeout=1)
            assert ssh2 is not None
            assert sftp2 is not None
            await pool.release((ssh2, sftp2))

    @pytest.mark.asyncio
    async def test_acquire_failure_releases_semaphore(self, sftp_hook_mocked, monkeypatch):
        from airflow.providers.sftp.hooks.sftp import SFTPHookAsync

        orig_get_conn = SFTPHookAsync._get_conn

        async def fail_get_conn(self):
            raise Exception("fail")

        monkeypatch.setattr(SFTPHookAsync, "_get_conn", fail_get_conn)

        async with SFTPClientPool("test_conn", pool_size=2) as pool:
            with pytest.raises(Exception, match="fail"):
                await pool.acquire()

            monkeypatch.setattr(SFTPHookAsync, "_get_conn", orig_get_conn)
            ssh, sftp = await pool.acquire()
            assert ssh is not None
            assert sftp is not None

    @pytest.mark.asyncio
    async def test_close(self, sftp_hook_mocked, mocker):
        pool = SFTPClientPool("test_conn", pool_size=2)
        close_spy = mocker.spy(pool, "close")

        async with pool:
            ssh, sftp = await pool.acquire()
            await pool.release((ssh, sftp))

        assert close_spy.call_count == 1

    @pytest.mark.asyncio
    async def test_close_warns_when_active_connections_exist(self, sftp_hook_mocked, caplog):
        class DummySSH:
            def __init__(self):
                self.closed = False

            def close(self):
                self.closed = True

        class DummySFTP:
            def __init__(self):
                self.exited = False

            def exit(self):
                self.exited = True

        pool = SFTPClientPool("warn_conn", pool_size=2)
        ssh = DummySSH()
        sftp = DummySFTP()
        pair = (ssh, sftp)
        pool._in_use.add(pair)

        with caplog.at_level("WARNING"):
            await pool.close()

        assert "Pool closed with 1 active connections" in caplog.text
        assert pair not in pool._in_use
        assert ssh.closed is True
        assert sftp.exited is True

    def test_pool_size_consistency_validation(self, sftp_hook_mocked):
        """Test that creating a pool with different pool_size for same conn_id raises ValueError."""
        # Create first instance with pool_size=2
        pool1 = SFTPClientPool("consistent_conn", pool_size=2)
        assert pool1.pool_size == 2

        # Attempt to create another instance with same conn_id but different pool_size should fail
        with pytest.raises(ValueError, match="has already been initialised with pool_size=2"):
            SFTPClientPool("consistent_conn", pool_size=5)

    def test_pool_size_consistency_with_default(self, sftp_hook_mocked, monkeypatch):
        """Test that creating a pool with default pool_size and then explicit different pool_size raises ValueError."""
        # Mock the config to return a specific default pool_size
        monkeypatch.setattr("airflow.providers.sftp.pools.sftp.conf.getint", lambda *args: 3)

        # Create first instance without explicit pool_size (uses default)
        pool1 = SFTPClientPool("default_conn")
        assert pool1.pool_size == 3

        # Attempt to create another instance with explicit different pool_size should fail
        with pytest.raises(ValueError, match="has already been initialised with pool_size=3"):
            SFTPClientPool("default_conn", pool_size=10)

    def test_pool_size_consistency_same_pool_size(self, sftp_hook_mocked):
        """Test that creating a pool with same pool_size for same conn_id succeeds."""
        # Create first instance with pool_size=4
        pool1 = SFTPClientPool("same_pool_conn", pool_size=4)
        assert pool1.pool_size == 4

        # Create another instance with same conn_id and same pool_size should succeed
        pool2 = SFTPClientPool("same_pool_conn", pool_size=4)
        assert pool2 is pool1  # Should be the same instance (singleton)
        assert pool2.pool_size == 4
