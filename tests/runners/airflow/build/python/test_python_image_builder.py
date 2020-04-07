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
import os
import shutil
import tempfile
from unittest import TestCase

import docker

from rainbow.build.image.python.python import PythonImageBuilder


class TestPythonImageBuilder(TestCase):
    __IMAGE_NAME = 'rainbow_image'
    __OUTPUT_PATH = '/mnt/vol1/my_output.json'

    def setUp(self) -> None:
        super().setUp()
        os.environ['TMPDIR'] = '/tmp'
        self.temp_dir = self.__temp_dir()
        self.temp_airflow_dir = self.__temp_dir()

    def tearDown(self) -> None:
        super().tearDown()
        self.__remove_dir(self.temp_dir)
        self.__remove_dir(self.temp_airflow_dir)

    def test_build(self):
        build_out = self.__test_build()

        self.assertTrue('RUN pip install -r requirements.txt' in build_out, 'Incorrect pip command')

        self.__test_image()

    def test_build_with_pip_conf(self):
        build_out = self.__test_build(use_pip_conf=True)

        self.assertTrue(
            'RUN --mount=type=secret,id=pip_config,dst=/etc/pip.conf  pip insta...' in build_out,
            'Incorrect pip command')

        self.__test_image()

    def __test_build(self, use_pip_conf=False):
        config = self.__create_conf('my_task')

        base_path = os.path.join(os.path.dirname(__file__), '../../rainbow')

        if use_pip_conf:
            config['pip_conf'] = os.path.join(base_path, 'pip.conf')

        builder = PythonImageBuilder(config=config,
                                     base_path=base_path,
                                     relative_source_path='helloworld',
                                     tag=self.__IMAGE_NAME)

        build_out = str(builder.build())

        return build_out

    def __test_image(self):
        docker_client = docker.from_env()
        docker_client.images.get(self.__IMAGE_NAME)

        cmd = 'export RAINBOW_INPUT="{\\"x\\": 1}" && ' + \
              'sh container-setup.sh && ' + \
              'python hello_world.py && ' + \
              f'sh container-teardown.sh {self.__OUTPUT_PATH}'
        cmds = ['/bin/bash', '-c', cmd]

        container_log = docker_client.containers.run(self.__IMAGE_NAME,
                                                     cmds,
                                                     volumes={
                                                         self.temp_dir: {
                                                             'bind': '/mnt/vol1',
                                                             'mode': 'rw'
                                                         },
                                                         self.temp_airflow_dir: {
                                                             'bind': '/airflow/xcom',
                                                             'mode': 'rw'},
                                                     })

        docker_client.close()

        print(container_log)

        self.assertEqual(
            "b\"Writing rainbow input..\\n" +
            "Hello world!\\n\\n" +
            "rainbow_input.json contents = {'x': 1}\\n" +
            "Writing rainbow output..\\n\"",
            str(container_log))

        with open(os.path.join(self.temp_airflow_dir, 'return.json')) as file:
            self.assertEqual(file.read(), '{"a": 1, "b": 2}')

    def __create_conf(self, task_id):
        return {
            'task': task_id,
            'cmd': 'foo bar',
            'image': self.__IMAGE_NAME,
            'source': 'baz',
            'input_type': 'my_input_type',
            'input_path': 'my_input',
            'no_cache': True,
            'output_path': self.__OUTPUT_PATH,
        }

    @staticmethod
    def __temp_dir():
        temp_dir = tempfile.mkdtemp()
        return temp_dir

    @staticmethod
    def __remove_dir(temp_dir):
        shutil.rmtree(temp_dir, ignore_errors=True)
