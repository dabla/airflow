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
"""
Example Airflow DAG for Google BigQuery service local file upload and external table creation.
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateExternalTableOperator,
    BigQueryDeleteDatasetOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.trigger_rule import TriggerRule
from system.openlineage.operator import OpenLineageTestOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
DAG_ID = "bigquery_operations"

DATASET_NAME = f"dataset_{DAG_ID}_{ENV_ID}"
DATA_SAMPLE_GCS_BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
DATA_SAMPLE_GCS_OBJECT_NAME = "bigquery/us-states/us-states.csv"
CSV_FILE_LOCAL_PATH = str(Path(__file__).parent / "resources" / "us-states.csv")


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "bigquery"],
) as dag:
    create_bucket = GCSCreateBucketOperator(task_id="create_bucket", bucket_name=DATA_SAMPLE_GCS_BUCKET_NAME)

    create_dataset = BigQueryCreateEmptyDatasetOperator(task_id="create_dataset", dataset_id=DATASET_NAME)

    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file_to_bucket",
        src=CSV_FILE_LOCAL_PATH,
        dst=DATA_SAMPLE_GCS_OBJECT_NAME,
        bucket=DATA_SAMPLE_GCS_BUCKET_NAME,
    )

    # [START howto_operator_bigquery_create_external_table]
    create_external_table = BigQueryCreateExternalTableOperator(
        task_id="create_external_table",
        destination_project_dataset_table=f"{DATASET_NAME}.external_table",
        bucket=DATA_SAMPLE_GCS_BUCKET_NAME,
        source_objects=[DATA_SAMPLE_GCS_OBJECT_NAME],
        schema_fields=[
            {"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"},
        ],
    )
    # [END howto_operator_bigquery_create_external_table]

    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset",
        dataset_id=DATASET_NAME,
        delete_contents=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=DATA_SAMPLE_GCS_BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    check_openlineage_events = OpenLineageTestOperator(
        task_id="check_openlineage_events",
        file_path=str(Path(__file__).parent / "resources" / "openlineage" / "bigquery_operations.json"),
    )

    (
        # TEST SETUP
        [create_bucket, create_dataset]
        # TEST BODY
        >> upload_file
        >> create_external_table
        # TEST TEARDOWN
        >> delete_dataset
        >> delete_bucket
        >> check_openlineage_events
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
