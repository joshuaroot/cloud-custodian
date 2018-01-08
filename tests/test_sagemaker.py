# Copyright 2017 Capital One Services, LLC
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
from __future__ import absolute_import, division, print_function, unicode_literals

from datetime import datetime
from dateutil import tz, zoneinfo

from .common import BaseTest, functional


class TestNotebookInstance(BaseTest):
    def test_list_notebook_instances(self):
        session_factory = self.replay_flight_data(
            'test_sagemaker_notebook_instances')
        p = self.load_policy({
            'name': 'list-notebook-instances',
            'resource': 'notebook-instance'
        }, session_factory=session_factory)
        resources = p.run()
        self.assertEqual(len(resources), 1)

    def test_tag_notebook_instances(self):
        session_factory = self.replay_flight_data(
            'test_sagemaker_tag_notebook_instances')
        p = self.load_policy({
            'name': 'tag-notebook-instances',
            'resource': 'notebook-instance',
            'filters': [{
                'tag:Category': 'absent'}],
            'actions': [{
                'type': 'tag',
                'key': 'Category',
                'value': 'TestValue'}]
        }, session_factory=session_factory)
        resources = p.run()
        self.assertEqual(len(resources), 1)

        client = session_factory().client('sagemaker')
        tags = client.list_tags(
            ResourceArn=resources[0]['NotebookInstanceArn'])['Tags']
        self.assertEqual(tags[0]['Value'], 'TestValue')

    def test_remove_tag_notebook_instance(self):
        session_factory = self.replay_flight_data(
            'test_sagemaker_remove_tag_notebook_instances')
        p = self.load_policy({
            'name': 'untag-notebook-instances',
            'resource': 'notebook-instance',
            'filters': [{
                'tag:Category': 'TestValue'}],
            'actions': [{
                'type': 'remove-tag',
                'tags': ['Category']}]
        }, session_factory=session_factory)
        resources = p.run()
        self.assertEqual(len(resources), 1)

        client = session_factory().client('sagemaker')
        tags = client.list_tags(
            ResourceArn=resources[0]['NotebookInstanceArn'])['Tags']
        self.assertEqual(len(tags), 0)

    def test_mark_for_op_notebook_instance(self):
        session_factory = self.replay_flight_data(
            'test_sagemaker_mark_for_op_notebook_instance')
        p = self.load_policy({
            'name': 'notebook-instances-untagged-delete',
            'resource': 'notebook-instance',
            'filters': [
                {'tag:Category': 'absent'},
                {'tag:custodian_cleanup': 'absent'}],
            'actions': [{
                'type': 'mark-for-op',
                'tag': 'custodian_cleanup',
                'op': 'delete',
                'days': 7}]}, session_factory=session_factory)
        resources = p.run()
        self.assertEqual(len(resources), 1)
        client = session_factory().client('sagemaker')
        tags = client.list_tags(
            ResourceArn=resources[0]['NotebookInstanceArn'])['Tags']
        self.assertTrue(tags[0]['Key'], 'custodian_cleanup')

    def test_marked_for_op_notebook_instance(self):
        session_factory = self.replay_flight_data(
            'test_sagemaker_marked_for_op_notebook_instance')
        p = self.load_policy({
            'name': 'notebook-instances-untagged-delete',
            'resource': 'notebook-instance',
            'filters': [{
                'type': 'marked-for-op',
                'tag': 'custodian_cleanup',
                'op': 'delete',
                'skew': 7}]}, session_factory=session_factory)
        resources = p.run()
        self.assertEqual(len(resources), 1)

    def test_stop_notebook_instance(self):
        session_factory = self.replay_flight_data(
            'test_sagemaker_stop_notebook_instance')
        p = self.load_policy({
            'name': 'delete-invalid-notebook-instance',
            'resource': 'notebook-instance',
            'filters': [
                {'tag:Category': 'absent'},
                {'NotebookInstanceStatus': 'InService'}],
            'actions': [{'type': 'stop'}]}, session_factory=session_factory)
        resources = p.run()
        self.assertTrue(len(resources), 1)

        client = session_factory().client('sagemaker')
        notebook = client.describe_notebook_instance(
            NotebookInstanceName=resources[0]['NotebookInstanceName'])
        self.assertTrue(notebook['NotebookInstanceStatus'], 'Stopping')

    def test_delete_notebook_instance(self):
        session_factory = self.replay_flight_data(
            'test_sagemaker_delete_notebook_instance')
        p = self.load_policy({
            'name': 'delete-invalid-notebook-instance',
            'resource': 'notebook-instance',
            'filters': [
                {'tag:Category': 'absent'},
                {'NotebookInstanceStatus': 'Stopped'}],
            'actions': [{'type': 'delete'}]}, session_factory=session_factory)
        resources = p.run()
        self.assertTrue(len(resources), 1)

        client = session_factory().client('sagemaker')
        notebook = client.describe_notebook_instance(
            NotebookInstanceName=resources[0]['NotebookInstanceName'])
        self.assertTrue(notebook['NotebookInstanceStatus'], 'Deleting')
