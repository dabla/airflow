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

import urllib
from collections.abc import Generator
from datetime import datetime
from unittest import mock

import pytest
import time_machine

from airflow.models import DagModel
from airflow.models.asset import (
    AssetAliasModel,
    AssetDagRunQueue,
    AssetEvent,
    AssetModel,
    DagScheduleAssetReference,
    TaskOutletAssetReference,
)
from airflow.models.dagrun import DagRun
from airflow.utils import timezone
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.db import clear_db_assets, clear_db_runs
from tests_common.test_utils.format_datetime import from_datetime_to_zulu_without_ms

DEFAULT_DATE = datetime(2020, 6, 11, 18, 0, 0, tzinfo=timezone.utc)

pytestmark = pytest.mark.db_test


def _create_assets(session, num: int = 2) -> None:
    assets = [
        AssetModel(
            id=i,
            name=f"simple{i}",
            uri=f"s3://bucket/key/{i}",
            group="asset",
            extra={"foo": "bar"},
            created_at=DEFAULT_DATE,
            updated_at=DEFAULT_DATE,
        )
        for i in range(1, 1 + num)
    ]
    session.add_all(assets)
    session.commit()


def _create_assets_with_sensitive_extra(session, num: int = 2) -> None:
    assets = [
        AssetModel(
            id=i,
            name=f"sensitive{i}",
            uri=f"s3://bucket/key/{i}",
            group="asset",
            extra={"password": "bar"},
            created_at=DEFAULT_DATE,
            updated_at=DEFAULT_DATE,
        )
        for i in range(1, 1 + num)
    ]
    session.add_all(assets)
    session.commit()


def _create_provided_asset(session, asset: AssetModel) -> None:
    session.add(asset)
    session.commit()


def _create_asset_aliases(session, num: int = 2) -> None:
    asset_aliases = [
        AssetAliasModel(
            id=i,
            name=f"simple{i}",
            group="alias",
        )
        for i in range(1, 1 + num)
    ]
    session.add_all(asset_aliases)
    session.commit()


def _create_provided_asset_alias(session, asset_alias: AssetAliasModel) -> None:
    session.add(asset_alias)
    session.commit()


def _create_assets_events(session, num: int = 2) -> None:
    assets_events = [
        AssetEvent(
            id=i,
            asset_id=i,
            extra={"foo": "bar"},
            source_task_id="source_task_id",
            source_dag_id="source_dag_id",
            source_run_id=f"source_run_id_{i}",
            timestamp=DEFAULT_DATE,
        )
        for i in range(1, 1 + num)
    ]
    session.add_all(assets_events)
    session.commit()


def _create_assets_events_with_sensitive_extra(session, num: int = 2) -> None:
    assets_events = [
        AssetEvent(
            id=i,
            asset_id=i,
            extra={"password": "bar"},
            source_task_id="source_task_id",
            source_dag_id="source_dag_id",
            source_run_id=f"source_run_id_{i}",
            timestamp=DEFAULT_DATE,
        )
        for i in range(1, 1 + num)
    ]
    session.add_all(assets_events)
    session.commit()


def _create_provided_asset_event(session, asset_event: AssetEvent) -> None:
    session.add(asset_event)
    session.commit()


def _create_dag_run(session, num: int = 2):
    dag_runs = [
        DagRun(
            dag_id="source_dag_id",
            run_id=f"source_run_id_{i}",
            run_type=DagRunType.MANUAL,
            logical_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            external_trigger=True,
            state=DagRunState.SUCCESS,
        )
        for i in range(1, 1 + num)
    ]
    for dag_run in dag_runs:
        dag_run.end_date = DEFAULT_DATE
    session.add_all(dag_runs)
    session.commit()


def _create_asset_dag_run(session, num: int = 2):
    for i in range(1, 1 + num):
        dag_run = session.query(DagRun).filter_by(run_id=f"source_run_id_{i}").first()
        asset_event = session.query(AssetEvent).filter_by(id=i).first()
        if dag_run and asset_event:
            dag_run.consumed_asset_events.append(asset_event)
    session.commit()


class TestAssets:
    @pytest.fixture
    def time_freezer(self) -> Generator:
        freezer = time_machine.travel(DEFAULT_DATE, tick=False)
        freezer.start()

        yield

        freezer.stop()

    @pytest.fixture(autouse=True)
    def setup(self) -> None:
        clear_db_assets()
        clear_db_runs()

    def teardown_method(self) -> None:
        clear_db_assets()
        clear_db_runs()

    @provide_session
    def create_assets(self, session, num: int = 2):
        _create_assets(session=session, num=num)

    @provide_session
    def create_assets_with_sensitive_extra(self, session, num: int = 2):
        _create_assets_with_sensitive_extra(session=session, num=num)

    @provide_session
    def create_provided_asset(self, session, asset: AssetModel):
        _create_provided_asset(session=session, asset=asset)

    @provide_session
    def create_assets_events(self, session, num: int = 2):
        _create_assets_events(session=session, num=num)

    @provide_session
    def create_assets_events_with_sensitive_extra(self, session, num: int = 2):
        _create_assets_events_with_sensitive_extra(session=session, num=num)

    @provide_session
    def create_provided_asset_event(self, session, asset_event: AssetEvent):
        _create_provided_asset_event(session=session, asset_event=asset_event)

    @provide_session
    def create_dag_run(self, session, num: int = 2):
        _create_dag_run(num=num, session=session)

    @provide_session
    def create_asset_dag_run(self, session, num: int = 2):
        _create_asset_dag_run(num=num, session=session)


class TestGetAssets(TestAssets):
    def test_should_respond_200(self, test_client, session):
        self.create_assets()
        assets = session.query(AssetModel).all()
        assert len(assets) == 2

        response = test_client.get("/public/assets")
        assert response.status_code == 200
        response_data = response.json()
        tz_datetime_format = from_datetime_to_zulu_without_ms(DEFAULT_DATE)
        assert response_data == {
            "assets": [
                {
                    "id": 1,
                    "name": "simple1",
                    "uri": "s3://bucket/key/1",
                    "group": "asset",
                    "extra": {"foo": "bar"},
                    "created_at": tz_datetime_format,
                    "updated_at": tz_datetime_format,
                    "consuming_dags": [],
                    "producing_tasks": [],
                    "aliases": [],
                },
                {
                    "id": 2,
                    "name": "simple2",
                    "uri": "s3://bucket/key/2",
                    "group": "asset",
                    "extra": {"foo": "bar"},
                    "created_at": tz_datetime_format,
                    "updated_at": tz_datetime_format,
                    "consuming_dags": [],
                    "producing_tasks": [],
                    "aliases": [],
                },
            ],
            "total_entries": 2,
        }

    def test_order_by_raises_400_for_invalid_attr(self, test_client, session):
        response = test_client.get("/public/assets?order_by=fake")

        assert response.status_code == 400
        msg = "Ordering with 'fake' is disallowed or the attribute does not exist on the model"
        assert response.json()["detail"] == msg

    @pytest.mark.parametrize(
        "params, expected_assets",
        [
            ({"name_pattern": "s3"}, {"s3://folder/key"}),
            ({"name_pattern": "bucket"}, {"gcp://bucket/key", "wasb://some_asset_bucket_/key"}),
            (
                {"name_pattern": "asset"},
                {"somescheme://asset/key", "wasb://some_asset_bucket_/key"},
            ),
            (
                {"name_pattern": ""},
                {
                    "gcp://bucket/key",
                    "s3://folder/key",
                    "somescheme://asset/key",
                    "wasb://some_asset_bucket_/key",
                },
            ),
        ],
    )
    @provide_session
    def test_filter_assets_by_name_pattern_works(self, test_client, params, expected_assets, session):
        asset1 = AssetModel("s3-folder-key", "s3://folder/key")
        asset2 = AssetModel("gcp-bucket-key", "gcp://bucket/key")
        asset3 = AssetModel("some-asset-key", "somescheme://asset/key")
        asset4 = AssetModel("wasb-some_asset_bucket_-key", "wasb://some_asset_bucket_/key")

        assets = [asset1, asset2, asset3, asset4]
        for a in assets:
            self.create_provided_asset(asset=a)

        response = test_client.get("/public/assets", params=params)
        assert response.status_code == 200
        asset_urls = {asset["uri"] for asset in response.json()["assets"]}
        assert expected_assets == asset_urls

    @pytest.mark.parametrize(
        "params, expected_assets",
        [
            ({"uri_pattern": "s3"}, {"s3://folder/key"}),
            ({"uri_pattern": "bucket"}, {"gcp://bucket/key", "wasb://some_asset_bucket_/key"}),
            (
                {"uri_pattern": "asset"},
                {"somescheme://asset/key", "wasb://some_asset_bucket_/key"},
            ),
            (
                {"uri_pattern": ""},
                {
                    "gcp://bucket/key",
                    "s3://folder/key",
                    "somescheme://asset/key",
                    "wasb://some_asset_bucket_/key",
                },
            ),
        ],
    )
    @provide_session
    def test_filter_assets_by_uri_pattern_works(self, test_client, params, expected_assets, session):
        asset1 = AssetModel("s3://folder/key")
        asset2 = AssetModel("gcp://bucket/key")
        asset3 = AssetModel("somescheme://asset/key")
        asset4 = AssetModel("wasb://some_asset_bucket_/key")

        assets = [asset1, asset2, asset3, asset4]
        for a in assets:
            self.create_provided_asset(asset=a)

        response = test_client.get("/public/assets", params=params)
        assert response.status_code == 200
        asset_urls = {asset["uri"] for asset in response.json()["assets"]}
        assert expected_assets == asset_urls

    @pytest.mark.parametrize("dag_ids, expected_num", [("dag1,dag2", 2), ("dag3", 1), ("dag2,dag3", 2)])
    @provide_session
    def test_filter_assets_by_dag_ids_works(self, test_client, dag_ids, expected_num, session):
        session.query(DagModel).delete()
        session.commit()
        dag1 = DagModel(dag_id="dag1")
        dag2 = DagModel(dag_id="dag2")
        dag3 = DagModel(dag_id="dag3")
        asset1 = AssetModel("s3://folder/key")
        asset2 = AssetModel("gcp://bucket/key")
        asset3 = AssetModel("somescheme://asset/key")
        dag_ref1 = DagScheduleAssetReference(dag_id="dag1", asset=asset1)
        dag_ref2 = DagScheduleAssetReference(dag_id="dag2", asset=asset2)
        task_ref1 = TaskOutletAssetReference(dag_id="dag3", task_id="task1", asset=asset3)
        session.add_all([asset1, asset2, asset3, dag1, dag2, dag3, dag_ref1, dag_ref2, task_ref1])
        session.commit()
        response = test_client.get(
            f"/public/assets?dag_ids={dag_ids}",
        )
        assert response.status_code == 200
        response_data = response.json()
        assert len(response_data["assets"]) == expected_num

    @pytest.mark.parametrize(
        "dag_ids, uri_pattern,expected_num",
        [("dag1,dag2", "folder", 1), ("dag3", "nothing", 0), ("dag2,dag3", "key", 2)],
    )
    @provide_session
    def test_filter_assets_by_dag_ids_and_uri_pattern_works(
        self, test_client, dag_ids, uri_pattern, expected_num, session
    ):
        session.query(DagModel).delete()
        session.commit()
        dag1 = DagModel(dag_id="dag1")
        dag2 = DagModel(dag_id="dag2")
        dag3 = DagModel(dag_id="dag3")
        asset1 = AssetModel("s3://folder/key")
        asset2 = AssetModel("gcp://bucket/key")
        asset3 = AssetModel("somescheme://asset/key")
        dag_ref1 = DagScheduleAssetReference(dag_id="dag1", asset=asset1)
        dag_ref2 = DagScheduleAssetReference(dag_id="dag2", asset=asset2)
        task_ref1 = TaskOutletAssetReference(dag_id="dag3", task_id="task1", asset=asset3)
        session.add_all([asset1, asset2, asset3, dag1, dag2, dag3, dag_ref1, dag_ref2, task_ref1])
        session.commit()
        response = test_client.get(
            f"/public/assets?dag_ids={dag_ids}&uri_pattern={uri_pattern}",
        )
        assert response.status_code == 200
        response_data = response.json()
        assert len(response_data["assets"]) == expected_num


class TestGetAssetsEndpointPagination(TestAssets):
    @pytest.mark.parametrize(
        "url, expected_asset_uris",
        [
            # Limit test data
            ("/public/assets?limit=1", ["s3://bucket/key/1"]),
            ("/public/assets?limit=100", [f"s3://bucket/key/{i}" for i in range(1, 101)]),
            # Offset test data
            ("/public/assets?offset=1", [f"s3://bucket/key/{i}" for i in range(2, 102)]),
            ("/public/assets?offset=3", [f"s3://bucket/key/{i}" for i in range(4, 104)]),
            # Limit and offset test data
            ("/public/assets?offset=3&limit=3", [f"s3://bucket/key/{i}" for i in [4, 5, 6]]),
        ],
    )
    def test_limit_and_offset(self, test_client, url, expected_asset_uris):
        self.create_assets(num=110)

        response = test_client.get(url)

        assert response.status_code == 200
        asset_uris = [asset["uri"] for asset in response.json()["assets"]]
        assert asset_uris == expected_asset_uris

    def test_should_respect_page_size_limit_default(self, test_client):
        self.create_assets(num=110)

        response = test_client.get("/public/assets")

        assert response.status_code == 200
        assert len(response.json()["assets"]) == 100


class TestAssetAliases:
    @pytest.fixture(autouse=True)
    def setup(self) -> None:
        clear_db_assets()
        clear_db_runs()

    def teardown_method(self) -> None:
        clear_db_assets()
        clear_db_runs()

    @provide_session
    def create_asset_aliases(self, num: int = 2, *, session):
        _create_asset_aliases(num=num, session=session)

    @provide_session
    def create_provided_asset_alias(self, asset_alias: AssetAliasModel, session):
        _create_provided_asset_alias(session=session, asset_alias=asset_alias)


class TestGetAssetAliases(TestAssetAliases):
    def test_should_respond_200(self, test_client, session):
        self.create_asset_aliases()
        asset_aliases = session.query(AssetAliasModel).all()
        assert len(asset_aliases) == 2

        response = test_client.get("/public/assets/aliases")
        assert response.status_code == 200
        response_data = response.json()
        assert response_data == {
            "asset_aliases": [
                {"id": 1, "name": "simple1", "group": "alias"},
                {"id": 2, "name": "simple2", "group": "alias"},
            ],
            "total_entries": 2,
        }

    def test_order_by_raises_400_for_invalid_attr(self, test_client, session):
        response = test_client.get("/public/assets/aliases?order_by=fake")

        assert response.status_code == 400
        msg = "Ordering with 'fake' is disallowed or the attribute does not exist on the model"
        assert response.json()["detail"] == msg

    @pytest.mark.parametrize(
        "params, expected_asset_aliases",
        [
            ({"name_pattern": "foo"}, {"foo1"}),
            ({"name_pattern": "1"}, {"foo1", "bar12"}),
            ({"uri_pattern": ""}, {"foo1", "bar12", "bar2", "bar3", "rex23"}),
        ],
    )
    @provide_session
    def test_filter_assets_by_name_pattern_works(self, test_client, params, expected_asset_aliases, session):
        asset_alias1 = AssetAliasModel(name="foo1")
        asset_alias2 = AssetAliasModel(name="bar12")
        asset_alias3 = AssetAliasModel(name="bar2")
        asset_alias4 = AssetAliasModel(name="bar3")
        asset_alias5 = AssetAliasModel(name="rex23")

        asset_aliases = [asset_alias1, asset_alias2, asset_alias3, asset_alias4, asset_alias5]
        for a in asset_aliases:
            self.create_provided_asset_alias(a)

        response = test_client.get("/public/assets/aliases", params=params)
        assert response.status_code == 200
        alias_names = {asset_alias["name"] for asset_alias in response.json()["asset_aliases"]}
        assert expected_asset_aliases == alias_names


class TestGetAssetAliasesEndpointPagination(TestAssetAliases):
    @pytest.mark.parametrize(
        "url, expected_asset_aliases",
        [
            # Limit test data
            ("/public/assets/aliases?limit=1", ["simple1"]),
            ("/public/assets/aliases?limit=100", [f"simple{i}" for i in range(1, 101)]),
            # Offset test data
            ("/public/assets/aliases?offset=1", [f"simple{i}" for i in range(2, 102)]),
            ("/public/assets/aliases?offset=3", [f"simple{i}" for i in range(4, 104)]),
            # Limit and offset test data
            ("/public/assets/aliases?offset=3&limit=3", ["simple4", "simple5", "simple6"]),
        ],
    )
    def test_limit_and_offset(self, test_client, url, expected_asset_aliases):
        self.create_asset_aliases(num=110)

        response = test_client.get(url)

        assert response.status_code == 200
        alias_names = [asset["name"] for asset in response.json()["asset_aliases"]]
        assert alias_names == expected_asset_aliases

    def test_should_respect_page_size_limit_default(self, test_client):
        self.create_asset_aliases(num=110)
        response = test_client.get("/public/assets/aliases")
        assert response.status_code == 200
        assert len(response.json()["asset_aliases"]) == 100


class TestGetAssetEvents(TestAssets):
    def test_should_respond_200(self, test_client, session):
        self.create_assets()
        self.create_assets_events()
        self.create_dag_run()
        self.create_asset_dag_run()
        assets = session.query(AssetEvent).all()
        assert len(assets) == 2
        response = test_client.get("/public/assets/events")
        assert response.status_code == 200
        response_data = response.json()
        assert response_data == {
            "asset_events": [
                {
                    "id": 1,
                    "asset_id": 1,
                    "uri": "s3://bucket/key/1",
                    "extra": {"foo": "bar"},
                    "source_task_id": "source_task_id",
                    "source_dag_id": "source_dag_id",
                    "source_run_id": "source_run_id_1",
                    "source_map_index": -1,
                    "created_dagruns": [
                        {
                            "run_id": "source_run_id_1",
                            "dag_id": "source_dag_id",
                            "logical_date": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                            "start_date": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                            "end_date": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                            "state": "success",
                            "data_interval_start": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                            "data_interval_end": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                        }
                    ],
                    "timestamp": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                },
                {
                    "id": 2,
                    "asset_id": 2,
                    "uri": "s3://bucket/key/2",
                    "extra": {"foo": "bar"},
                    "source_task_id": "source_task_id",
                    "source_dag_id": "source_dag_id",
                    "source_run_id": "source_run_id_2",
                    "source_map_index": -1,
                    "created_dagruns": [
                        {
                            "run_id": "source_run_id_2",
                            "dag_id": "source_dag_id",
                            "logical_date": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                            "start_date": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                            "end_date": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                            "state": "success",
                            "data_interval_start": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                            "data_interval_end": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                        }
                    ],
                    "timestamp": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                },
            ],
            "total_entries": 2,
        }

    @pytest.mark.parametrize(
        "params, total_entries",
        [
            ({"asset_id": "2"}, 1),
            ({"source_dag_id": "source_dag_id"}, 2),
            ({"source_task_id": "source_task_id"}, 2),
            ({"source_run_id": "source_run_id_1"}, 1),
            ({"source_map_index": "-1"}, 2),
        ],
    )
    @provide_session
    def test_filtering(self, test_client, params, total_entries, session):
        self.create_assets()
        self.create_assets_events()
        self.create_dag_run()
        self.create_asset_dag_run()
        response = test_client.get("/public/assets/events", params=params)
        assert response.status_code == 200
        assert response.json()["total_entries"] == total_entries

    def test_order_by_raises_400_for_invalid_attr(self, test_client, session):
        response = test_client.get("/public/assets/events?order_by=fake")

        assert response.status_code == 400
        msg = "Ordering with 'fake' is disallowed or the attribute does not exist on the model"
        assert response.json()["detail"] == msg

    @pytest.mark.parametrize(
        "params, expected_asset_uris",
        [
            # Limit test data
            ({"limit": "1"}, ["s3://bucket/key/1"]),
            ({"limit": "100"}, [f"s3://bucket/key/{i}" for i in range(1, 101)]),
            # Offset test data
            ({"offset": "1"}, [f"s3://bucket/key/{i}" for i in range(2, 102)]),
            ({"offset": "3"}, [f"s3://bucket/key/{i}" for i in range(4, 104)]),
        ],
    )
    def test_limit_and_offset(self, test_client, params, expected_asset_uris):
        self.create_assets(num=110)
        self.create_assets_events(num=110)
        self.create_dag_run(num=110)
        self.create_asset_dag_run(num=110)

        response = test_client.get("/public/assets/events", params=params)

        assert response.status_code == 200
        asset_uris = [asset["uri"] for asset in response.json()["asset_events"]]
        assert asset_uris == expected_asset_uris

    @pytest.mark.usefixtures("time_freezer")
    @pytest.mark.enable_redact
    def test_should_mask_sensitive_extra(self, test_client, session):
        self.create_assets_with_sensitive_extra()
        self.create_assets_events_with_sensitive_extra()
        self.create_dag_run()
        self.create_asset_dag_run()
        response = test_client.get("/public/assets/events")
        assert response.status_code == 200
        response_data = response.json()
        assert response_data == {
            "asset_events": [
                {
                    "id": 1,
                    "asset_id": 1,
                    "uri": "s3://bucket/key/1",
                    "extra": {"password": "***"},
                    "source_task_id": "source_task_id",
                    "source_dag_id": "source_dag_id",
                    "source_run_id": "source_run_id_1",
                    "source_map_index": -1,
                    "created_dagruns": [
                        {
                            "run_id": "source_run_id_1",
                            "dag_id": "source_dag_id",
                            "logical_date": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                            "start_date": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                            "end_date": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                            "state": "success",
                            "data_interval_start": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                            "data_interval_end": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                        }
                    ],
                    "timestamp": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                },
                {
                    "id": 2,
                    "asset_id": 2,
                    "uri": "s3://bucket/key/2",
                    "extra": {"password": "***"},
                    "source_task_id": "source_task_id",
                    "source_dag_id": "source_dag_id",
                    "source_run_id": "source_run_id_2",
                    "source_map_index": -1,
                    "created_dagruns": [
                        {
                            "run_id": "source_run_id_2",
                            "dag_id": "source_dag_id",
                            "logical_date": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                            "start_date": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                            "end_date": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                            "state": "success",
                            "data_interval_start": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                            "data_interval_end": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                        }
                    ],
                    "timestamp": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                },
            ],
            "total_entries": 2,
        }


class TestGetAssetEndpoint(TestAssets):
    @pytest.mark.parametrize(
        "url",
        [
            urllib.parse.quote(
                "s3://bucket/key/1", safe=""
            ),  # api should cover raw as well as unquoted case like legacy
            "s3://bucket/key/1",
        ],
    )
    @provide_session
    def test_should_respond_200(self, test_client, url, session):
        self.create_assets(num=1)
        assert session.query(AssetModel).count() == 1
        tz_datetime_format = from_datetime_to_zulu_without_ms(DEFAULT_DATE)
        with assert_queries_count(6):
            response = test_client.get(
                f"/public/assets/{url}",
            )
        assert response.status_code == 200
        assert response.json() == {
            "id": 1,
            "name": "simple1",
            "uri": "s3://bucket/key/1",
            "group": "asset",
            "extra": {"foo": "bar"},
            "created_at": tz_datetime_format,
            "updated_at": tz_datetime_format,
            "consuming_dags": [],
            "producing_tasks": [],
            "aliases": [],
        }

    def test_should_respond_404(self, test_client):
        response = test_client.get(
            f"/public/assets/{urllib.parse.quote('s3://bucket/key', safe='')}",
        )
        assert response.status_code == 404
        assert response.json()["detail"] == "The Asset with uri: `s3://bucket/key` was not found"

    @pytest.mark.usefixtures("time_freezer")
    @pytest.mark.enable_redact
    def test_should_mask_sensitive_extra(self, test_client, session):
        self.create_assets_with_sensitive_extra()
        tz_datetime_format = from_datetime_to_zulu_without_ms(DEFAULT_DATE)
        uri = "s3://bucket/key/1"
        response = test_client.get(
            f"/public/assets/{uri}",
        )
        assert response.status_code == 200
        assert response.json() == {
            "id": 1,
            "name": "sensitive1",
            "uri": "s3://bucket/key/1",
            "group": "asset",
            "extra": {"password": "***"},
            "created_at": tz_datetime_format,
            "updated_at": tz_datetime_format,
            "consuming_dags": [],
            "producing_tasks": [],
            "aliases": [],
        }


class TestQueuedEventEndpoint(TestAssets):
    def _create_asset_dag_run_queues(self, dag_id, asset_id, session):
        adrq = AssetDagRunQueue(target_dag_id=dag_id, asset_id=asset_id)
        session.add(adrq)
        session.commit()
        return adrq


class TestGetDagAssetQueuedEvents(TestQueuedEventEndpoint):
    @pytest.mark.usefixtures("time_freezer")
    def test_should_respond_200(self, test_client, session, create_dummy_dag):
        dag, _ = create_dummy_dag()
        dag_id = dag.dag_id
        self.create_assets(session=session, num=1)
        asset_id = 1
        self._create_asset_dag_run_queues(dag_id, asset_id, session)

        response = test_client.get(
            f"/public/dags/{dag_id}/assets/queuedEvents",
        )

        assert response.status_code == 200
        assert response.json() == {
            "queued_events": [
                {
                    "created_at": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                    "uri": "s3://bucket/key/1",
                    "dag_id": "dag",
                }
            ],
            "total_entries": 1,
        }

    def test_should_respond_404(self, test_client):
        dag_id = "not_exists"

        response = test_client.get(
            f"/public/dags/{dag_id}/assets/queuedEvents",
        )

        assert response.status_code == 404
        assert response.json()["detail"] == "Queue event with dag_id: `not_exists` was not found"


class TestDeleteDagDatasetQueuedEvents(TestQueuedEventEndpoint):
    @pytest.mark.usefixtures("time_freezer")
    def test_should_respond_204(self, test_client, session, create_dummy_dag):
        dag, _ = create_dummy_dag()
        dag_id = dag.dag_id
        self.create_assets(session=session, num=1)
        asset_id = 1
        self._create_asset_dag_run_queues(dag_id, asset_id, session)
        adrqs = session.query(AssetDagRunQueue).all()
        assert len(adrqs) == 1

        response = test_client.delete(
            f"/public/dags/{dag_id}/assets/queuedEvents",
        )

        assert response.status_code == 204
        adrqs = session.query(AssetDagRunQueue).all()
        assert len(adrqs) == 0

    def test_should_respond_404_invalid_dag(self, test_client):
        dag_id = "not_exists"

        response = test_client.delete(
            f"/public/dags/{dag_id}/assets/queuedEvents",
        )

        assert response.status_code == 404
        assert response.json()["detail"] == "Queue event with dag_id: `not_exists` was not found"

    def test_should_respond_404_valid_dag_no_adrq(self, test_client, session, create_dummy_dag):
        dag, _ = create_dummy_dag()
        dag_id = dag.dag_id
        self.create_assets(session=session, num=1)
        adrqs = session.query(AssetDagRunQueue).all()
        assert len(adrqs) == 0

        response = test_client.delete(
            f"/public/dags/{dag_id}/assets/queuedEvents",
        )

        assert response.status_code == 404
        assert response.json()["detail"] == "Queue event with dag_id: `dag` was not found"


class TestPostAssetEvents(TestAssets):
    @pytest.mark.usefixtures("time_freezer")
    def test_should_respond_200(self, test_client, session):
        self.create_assets()
        event_payload = {"uri": "s3://bucket/key/1", "extra": {"foo": "bar"}}
        response = test_client.post("/public/assets/events", json=event_payload)
        assert response.status_code == 200
        assert response.json() == {
            "id": mock.ANY,
            "asset_id": 1,
            "uri": "s3://bucket/key/1",
            "extra": {"foo": "bar", "from_rest_api": True},
            "source_task_id": None,
            "source_dag_id": None,
            "source_run_id": None,
            "source_map_index": -1,
            "created_dagruns": [],
            "timestamp": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
        }

    def test_invalid_attr_not_allowed(self, test_client, session):
        self.create_assets(session)
        event_invalid_payload = {"asset_uri": "s3://bucket/key/1", "extra": {"foo": "bar"}, "fake": {}}
        response = test_client.post("/public/assets/events", json=event_invalid_payload)

        assert response.status_code == 422

    @pytest.mark.usefixtures("time_freezer")
    @pytest.mark.enable_redact
    def test_should_mask_sensitive_extra(self, test_client, session):
        self.create_assets(session)
        event_payload = {"uri": "s3://bucket/key/1", "extra": {"password": "bar"}}
        response = test_client.post("/public/assets/events", json=event_payload)
        assert response.status_code == 200
        assert response.json() == {
            "id": mock.ANY,
            "asset_id": 1,
            "uri": "s3://bucket/key/1",
            "extra": {"password": "***", "from_rest_api": True},
            "source_task_id": None,
            "source_dag_id": None,
            "source_run_id": None,
            "source_map_index": -1,
            "created_dagruns": [],
            "timestamp": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
        }


class TestGetAssetQueuedEvents(TestQueuedEventEndpoint):
    @pytest.mark.usefixtures("time_freezer")
    def test_should_respond_200(self, test_client, session, create_dummy_dag):
        dag, _ = create_dummy_dag()
        dag_id = dag.dag_id
        self.create_assets(session=session, num=1)
        uri = "s3://bucket/key/1"
        asset_id = 1
        self._create_asset_dag_run_queues(dag_id, asset_id, session)

        response = test_client.get(
            f"/public/assets/queuedEvents/{uri}",
        )
        assert response.status_code == 200
        assert response.json() == {
            "queued_events": [
                {
                    "created_at": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                    "uri": "s3://bucket/key/1",
                    "dag_id": "dag",
                }
            ],
            "total_entries": 1,
        }

    def test_should_respond_404(self, test_client):
        uri = "not_exists"

        response = test_client.get(
            f"/public/assets/queuedEvents/{uri}",
        )

        assert response.status_code == 404
        assert response.json()["detail"] == "Queue event with uri: `not_exists` was not found"


class TestDeleteAssetQueuedEvents(TestQueuedEventEndpoint):
    @pytest.mark.usefixtures("time_freezer")
    def test_should_respond_204(self, test_client, session, create_dummy_dag):
        dag, _ = create_dummy_dag()
        dag_id = dag.dag_id
        uri = "s3://bucket/key/1"
        self.create_assets(session=session, num=1)
        asset_id = 1
        self._create_asset_dag_run_queues(dag_id, asset_id, session)

        response = test_client.delete(
            f"/public/assets/queuedEvents/{uri}",
        )
        assert response.status_code == 204
        assert session.query(AssetDagRunQueue).filter_by(asset_id=1).first() is None

    def test_should_respond_404(self, test_client):
        uri = "not_exists"

        response = test_client.delete(
            f"/public/assets/queuedEvents/{uri}",
        )

        assert response.status_code == 404
        assert response.json()["detail"] == "Queue event with uri: `not_exists` was not found"


class TestDeleteDagAssetQueuedEvent(TestQueuedEventEndpoint):
    def test_delete_should_respond_204(self, test_client, session, create_dummy_dag):
        dag, _ = create_dummy_dag()
        dag_id = dag.dag_id
        asset_uri = "s3://bucket/key/1"
        self.create_assets(session=session, num=1)
        asset_id = 1

        self._create_asset_dag_run_queues(dag_id, asset_id, session)
        adrq = session.query(AssetDagRunQueue).all()
        assert len(adrq) == 1

        response = test_client.delete(
            f"/public/dags/{dag_id}/assets/queuedEvents/{asset_uri}",
        )

        assert response.status_code == 204
        adrq = session.query(AssetDagRunQueue).all()
        assert len(adrq) == 0

    def test_should_respond_404(self, test_client):
        dag_id = "not_exists"
        asset_uri = "not_exists"

        response = test_client.delete(
            f"/public/dags/{dag_id}/assets/queuedEvents/{asset_uri}",
        )

        assert response.status_code == 404
        assert (
            response.json()["detail"]
            == "Queued event with dag_id: `not_exists` and asset uri: `not_exists` was not found"
        )
