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

from typing import TYPE_CHECKING, Any
from unittest.mock import patch

import pytest

from airflow.configuration import conf
from airflow.models.baseoperator import BaseOperator, ExecutorSafeguard
from airflow.operators.python import PythonOperator, task
from airflow.utils.state import DagRunState

if TYPE_CHECKING:
    from airflow.utils.context import Context


class HelloWorldOperator(BaseOperator):
    def execute(self, context: Context) -> Any:
        return f"Hello {self.owner}!"


class TestExecutorSafeguard:
    def setup_method(self):
        ExecutorSafeguard.test_mode = False

    def teardown_method(self, method):
        ExecutorSafeguard.test_mode = conf.getboolean("core", "unit_test_mode")

    @pytest.mark.db_test
    def test_executor_when_classic_operator_called_from_dag(self, dag_maker):
        with dag_maker() as dag:
            HelloWorldOperator(task_id="hello_operator")

        dag_run = dag.test()
        assert dag_run.state == DagRunState.SUCCESS

    @pytest.mark.db_test
    def test_executor_when_classic_operator_called_from_decorated_task(self, dag_maker):
        with dag_maker() as dag:

            @task(task_id="task_id", dag=dag)
            def say_hello(**context):
                operator = HelloWorldOperator(task_id="hello_operator")
                return operator.execute(context=context)

            say_hello()

        dag_run = dag.test()
        assert dag_run.state == DagRunState.FAILED

    @pytest.mark.db_test
    @patch.object(HelloWorldOperator, "log")
    def test_executor_when_classic_operator_called_from_decorated_task_with_allow_mixin(
        self,
        mock_log,
        dag_maker,
    ):
        with dag_maker() as dag:

            @task(task_id="task_id", dag=dag)
            def say_hello(**context):
                operator = HelloWorldOperator(task_id="hello_operator", allow_mixin=True)
                return operator.execute(context=context)

            say_hello()

        dag_run = dag.test()
        assert dag_run.state == DagRunState.SUCCESS
        mock_log.warning.assert_called_once_with(
            "HelloWorldOperator.execute cannot be called outside TaskInstance!"
        )

    @pytest.mark.db_test
    def test_executor_when_classic_operator_called_from_python_operator(
        self,
        dag_maker,
    ):
        with dag_maker() as dag:

            def say_hello(**context):
                operator = HelloWorldOperator(task_id="hello_operator")
                return operator.execute(context=context)

            PythonOperator(
                task_id="say_hello",
                dag=dag,
                python_callable=say_hello,
            )

        dag_run = dag.test()
        assert dag_run.state == DagRunState.FAILED

    @pytest.mark.db_test
    @patch.object(HelloWorldOperator, "log")
    def test_executor_when_classic_operator_called_from_python_operator_with_allow_mixin(
        self,
        mock_log,
        dag_maker,
    ):
        with dag_maker() as dag:

            def say_hello(**context):
                operator = HelloWorldOperator(task_id="hello_operator", allow_mixin=True)
                return operator.execute(context=context)

            PythonOperator(
                task_id="say_hello",
                dag=dag,
                python_callable=say_hello,
            )

        dag_run = dag.test()
        assert dag_run.state == DagRunState.SUCCESS
        mock_log.warning.assert_called_once_with(
            "HelloWorldOperator.execute cannot be called outside TaskInstance!"
        )
