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
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator

from rainbow.runners.airflow.model import task
from rainbow.runners.airflow.operators.cloudformation import CloudFormationDeleteStackOperator, \
    CloudFormationDeleteStackSensor


class DeleteCloudFormationStackTask(task.Task):
    """
    Deletes cloud_formation stack.
    """

    def __init__(self, dag, pipeline_name, parent, config, trigger_rule):
        super().__init__(dag, pipeline_name, parent, config, trigger_rule)
        self.stack_name = config['name']

    def apply_task_to_dag(self):
        check_dags_queued_task = BranchPythonOperator(
            task_id='is_dag_queue_empty',
            python_callable=self.__queued_dag_runs_exists,
            provide_context=True,
            trigger_rule=self.trigger_rule,
            dag=self.dag
        )

        delete_stack_task = CloudFormationDeleteStackOperator(
            task_id='delete_cloudformation_stack',
            params={'StackName': self.stack_name},
            dag=self.dag
        )

        delete_stack_sensor = CloudFormationDeleteStackSensor(
            task_id='cloudformation_watch_cluster_delete',
            stack_name=self.stack_name,
            dag=self.dag
        )

        stack_delete_end_task = DummyOperator(
            task_id='stack_delete_end',
            dag=self.dag
        )

        if self.parent:
            self.parent.set_down_sream(check_dags_queued_task)

        check_dags_queued_task.set_downstream(stack_delete_end_task)
        check_dags_queued_task.set_downstream(delete_stack_task)
        delete_stack_task.set_downstream(delete_stack_sensor)

    def __queued_dag_runs_exists(self, **kwargs):
        if self.dag.get_num_active_runs() > 1:
            return 'stack_delete_end'
        else:
            return 'delete_cloudformation_stack'
