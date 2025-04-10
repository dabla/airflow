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
        "package-name": "apache-airflow-providers-smtp",
        "name": "Simple Mail Transfer Protocol (SMTP)",
        "description": "`Simple Mail Transfer Protocol (SMTP) <https://tools.ietf.org/html/rfc5321>`__\n",
        "integrations": [
            {
                "integration-name": "Simple Mail Transfer Protocol (SMTP)",
                "external-doc-url": "https://tools.ietf.org/html/rfc5321",
                "logo": "/docs/integration-logos/SMTP.png",
                "tags": ["protocol"],
            }
        ],
        "operators": [
            {
                "integration-name": "Simple Mail Transfer Protocol (SMTP)",
                "python-modules": ["airflow.providers.smtp.operators.smtp"],
            }
        ],
        "hooks": [
            {
                "integration-name": "Simple Mail Transfer Protocol (SMTP)",
                "python-modules": ["airflow.providers.smtp.hooks.smtp"],
            }
        ],
        "connection-types": [
            {"hook-class-name": "airflow.providers.smtp.hooks.smtp.SmtpHook", "connection-type": "smtp"}
        ],
        "notifications": ["airflow.providers.smtp.notifications.smtp.SmtpNotifier"],
    }
