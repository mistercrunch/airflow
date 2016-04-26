# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from airflow.ti_deps.deps.base_ti_dep import BaseTIDep

from airflow.utils.db import provide_session


class DagTISlotsAvailableDep(BaseTIDep):
    NAME = "Task Instance Slots Available"

    @provide_session
    def get_dep_statuses(self, ti, session, dep_context):
        if ti.task.dag.concurrency_reached:
            yield self._failing_status(
                reason="The maximum number of running tasks ({0}) for this task's DAG "
                       "'{1}' has been reached.".format(ti.dag_id,
                                                        ti.task.dag.concurrency))
