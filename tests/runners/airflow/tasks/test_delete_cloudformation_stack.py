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

import unittest
from unittest import TestCase

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator

from rainbow.runners.airflow.operators.cloudformation import CloudFormationDeleteStackOperator, \
    CloudFormationDeleteStackSensor
from rainbow.runners.airflow.tasks import delete_cloudformation_stack
from tests.util import dag_test_utils


class TestDeleteCloudFormationStackTask(TestCase):

    def test_apply_task_to_dag(self):
        dag = dag_test_utils.create_dag()

        task_id = 'my_task'

        config = self.__create_conf(task_id)

        task0 = delete_cloudformation_stack.DeleteCloudFormationStackTask(dag, 'my_pipeline', None, config,
                                                                          'all_done')
        task0.apply_task_to_dag()

        self.assertEqual(len(dag.tasks), 4)

        self.__test_check_dags_queued_task(task0.dag.tasks[0])
        self.__test_delete_stack_task(task0.dag.tasks[1])
        self.__test_delete_stack_sensor(task0.dag.tasks[2])
        self.__test_stack_delete_end_task(task0.dag.tasks[3])

    def __test_check_dags_queued_task(self, task):
        self.assertIsInstance(task, BranchPythonOperator)
        self.assertEqual(task.task_id, 'is_dag_queue_empty')
        self.assertEqual(task.trigger_rule, 'all_done')
        downstream_lst = task.downstream_list
        # The order of the downstream tasks here does not matter. sorting just to keep it deterministic for tests
        downstream_lst.sort()
        self.assertEqual(len(downstream_lst), 2)

        self.__test_delete_stack_task(downstream_lst[0])
        self.__test_stack_delete_end_task(downstream_lst[1])

    def __test_delete_stack_task(self, task):
        self.assertIsInstance(task, CloudFormationDeleteStackOperator)
        self.assertEqual(task.task_id, 'delete_cloudformation_stack')

    def __test_delete_stack_sensor(self, task):
        self.assertIsInstance(task, CloudFormationDeleteStackSensor)
        self.assertEqual(task.task_id, 'cloudformation_watch_cluster_delete')

    def __test_stack_delete_end_task(self, task):
        self.assertIsInstance(task, DummyOperator)
        self.assertEqual(task.task_id, 'stack_delete_end')

    @staticmethod
    def __create_conf(task_id):
        return {
            'name': task_id
        }


if __name__ == '__main__':
    unittest.main()
