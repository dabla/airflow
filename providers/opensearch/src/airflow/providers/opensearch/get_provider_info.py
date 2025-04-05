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

# NOTE! THIS FILE IS AUTOMATICALLY GENERATED AND WILL BE OVERWRITTEN!
#
# IF YOU WANT TO MODIFY THIS FILE, YOU SHOULD MODIFY THE TEMPLATE
# `get_provider_info_TEMPLATE.py.jinja2` IN the `dev/breeze/src/airflow_breeze/templates` DIRECTORY


def get_provider_info():
    return {
        "package-name": "apache-airflow-providers-opensearch",
        "name": "OpenSearch",
        "description": "`OpenSearch <https://opensearch.org/>`__\n",
        "state": "ready",
        "source-date-epoch": 1743477861,
        "versions": [
            "1.6.2",
            "1.6.1",
            "1.6.0",
            "1.5.0",
            "1.4.0",
            "1.3.0",
            "1.2.1",
            "1.2.0",
            "1.1.2",
            "1.1.1",
            "1.1.0",
            "1.0.0",
        ],
        "integrations": [
            {
                "integration-name": "OpenSearch",
                "external-doc-url": "https://opensearch.org/",
                "how-to-guide": ["/docs/apache-airflow-providers-opensearch/operators/opensearch.rst"],
                "logo": "/docs/integration-logos/opensearch.png",
                "tags": ["software"],
            }
        ],
        "hooks": [
            {
                "integration-name": "OpenSearch",
                "python-modules": ["airflow.providers.opensearch.hooks.opensearch"],
            }
        ],
        "operators": [
            {
                "integration-name": "OpenSearch",
                "python-modules": ["airflow.providers.opensearch.operators.opensearch"],
            }
        ],
        "connection-types": [
            {
                "hook-class-name": "airflow.providers.opensearch.hooks.opensearch.OpenSearchHook",
                "connection-type": "opensearch",
            }
        ],
        "logging": ["airflow.providers.opensearch.log.os_task_handler.OpensearchTaskHandler"],
        "config": {
            "opensearch": {
                "description": None,
                "options": {
                    "host": {
                        "description": "Opensearch host\n",
                        "version_added": "1.5.0",
                        "type": "string",
                        "example": None,
                        "default": "",
                    },
                    "port": {
                        "description": "The port number of Opensearch host\n",
                        "version_added": "1.5.0",
                        "type": "integer",
                        "example": None,
                        "default": "",
                    },
                    "username": {
                        "description": "The username for connecting to Opensearch\n",
                        "version_added": "1.5.0",
                        "type": "string",
                        "sensitive": True,
                        "example": None,
                        "default": "",
                    },
                    "password": {
                        "description": "The password for connecting to Opensearch\n",
                        "version_added": "1.5.0",
                        "type": "string",
                        "sensitive": True,
                        "example": None,
                        "default": "",
                    },
                    "log_id_template": {
                        "description": "Format of the log_id, which is used to query for a given tasks logs\n",
                        "version_added": "1.5.0",
                        "type": "string",
                        "example": None,
                        "is_template": True,
                        "default": "{dag_id}-{task_id}-{run_id}-{map_index}-{try_number}",
                    },
                    "end_of_log_mark": {
                        "description": "Used to mark the end of a log stream for a task\n",
                        "version_added": "1.5.0",
                        "type": "string",
                        "example": None,
                        "default": "end_of_log",
                    },
                    "write_stdout": {
                        "description": "Write the task logs to the stdout of the worker, rather than the default files\n",
                        "version_added": "1.5.0",
                        "type": "string",
                        "example": None,
                        "default": "False",
                    },
                    "json_format": {
                        "description": "Instead of the default log formatter, write the log lines as JSON\n",
                        "version_added": "1.5.0",
                        "type": "string",
                        "example": None,
                        "default": "False",
                    },
                    "json_fields": {
                        "description": "Log fields to also attach to the json output, if enabled\n",
                        "version_added": "1.5.0",
                        "type": "string",
                        "example": None,
                        "default": "asctime, filename, lineno, levelname, message",
                    },
                    "host_field": {
                        "description": "The field where host name is stored (normally either `host` or `host.name`)\n",
                        "version_added": "1.5.0",
                        "type": "string",
                        "example": None,
                        "default": "host",
                    },
                    "offset_field": {
                        "description": "The field where offset is stored (normally either `offset` or `log.offset`)\n",
                        "version_added": "1.5.0",
                        "type": "string",
                        "example": None,
                        "default": "offset",
                    },
                    "index_patterns": {
                        "description": "Comma separated list of index patterns to use when searching for logs (default: `_all`).\nThe index_patterns_callable takes precedence over this.\n",
                        "version_added": "1.5.0",
                        "type": "string",
                        "example": "something-*",
                        "default": "_all",
                    },
                    "index_patterns_callable": {
                        "description": "A string representing the full path to the Python callable path which accept TI object and\nreturn comma separated list of index patterns. This will takes precedence over index_patterns.\n",
                        "version_added": "1.5.0",
                        "type": "string",
                        "example": "module.callable",
                        "default": "",
                    },
                },
            },
            "opensearch_configs": {
                "description": None,
                "options": {
                    "http_compress": {
                        "description": None,
                        "version_added": "1.5.0",
                        "type": "string",
                        "example": None,
                        "default": "False",
                    },
                    "use_ssl": {
                        "description": None,
                        "version_added": "1.5.0",
                        "type": "string",
                        "example": None,
                        "default": "False",
                    },
                    "verify_certs": {
                        "description": None,
                        "version_added": "1.5.0",
                        "type": "string",
                        "example": None,
                        "default": "False",
                    },
                    "ssl_assert_hostname": {
                        "description": None,
                        "version_added": "1.5.0",
                        "type": "string",
                        "example": None,
                        "default": "False",
                    },
                    "ssl_show_warn": {
                        "description": None,
                        "version_added": "1.5.0",
                        "type": "string",
                        "example": None,
                        "default": "False",
                    },
                    "ca_certs": {
                        "description": None,
                        "version_added": "1.5.0",
                        "type": "string",
                        "example": None,
                        "default": "",
                    },
                },
            },
        },
        "dependencies": ["apache-airflow>=2.9.0", "opensearch-py>=2.2.0"],
        "devel-dependencies": [],
    }
