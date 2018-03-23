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

from c7n.manager import resources
from c7n.query import QueryResourceManager
from c7n.actions import BaseAction
from c7n.utils import type_schema, local_session


@resources.register('batch-compute')
class ComputeEnvironment(QueryResourceManager):

    class resource_type(object):
        service = 'batch'
        filter_name = 'computeEnvironments'
        filter_type = 'list'
        dimension = None
        id = name = "computeEnvironmentName"
        enum_spec = (
            'describe_compute_environments', 'computeEnvironments', None)


@ComputeEnvironment.action_registry.register('update-environment')
class UpdateComputeEnvironment(BaseAction):
    """Updates an AWS batch compute environment

    :example:

    .. code-block: yaml

        policies:
          - name: update-environments
            resource: batch-compute
            filters:
              - computeResources.desiredvCpus: 0
              - state: ENABLED
            actions:
              - type: disable
                state: DISABLED
    """
    schema = {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'type': {'enum': ['update-environment']},
            'computeEnvironment': {'type': 'string'},
            'state': {'type': 'enum', 'items': ['ENABLED', 'DISABLED']},
            'computeResources': {
                'type': 'object',
                'additionalProperties': False,
                'properties': {
                    'minvCpus': {'type': 'integer'},
                    'maxvCpus': {'type': 'integer'},
                    'desiredvCpus': {'type': 'integer'}
                }
            },
            'serviceRole': {'type': 'string'}
        }
    }
    permissions = ('batch:UpdateComputeEnvironment',)

    def process(self, resources):
        client = local_session(self.manager.session_factory).client('batch')
        params = dict(self.data)
        params.pop('type')
        for r in resources:
            params['computeEnvironment'] = r['computeEnvironmentName']
            client.update_compute_environment(**params)


@ComputeEnvironment.action_registry.register('delete')
class DeleteComputeEnvironment(BaseAction):
    """Delete an AWS batch compute environment

    :example:

    .. code-block: yaml

        policies:
          - name: delete-environments
            resource: batch-compute
            filters:
              - computeResources.desiredvCpus: 0
            action:
              - type: delete
    """
    schema = type_schema('delete')
    permissions = ('batch:DeleteComputeEnvironment',)

    def delete_environment(self, r):
        client = local_session(self.manager.session_factory).client('batch')
        client.delete_compute_environment(
            computeEnvironment=r['computeEnvironmentName'])

    def process(self, resources):
        orig_length = len(resources)
        resources = [r for r in resources if r['state'] == 'DISABLED']
        self.log.info(
            "%s %d of %d batch-compute environments with state 'DISABLED'" % (
                self.__class__.__name__, len(resources), orig_length))
        with self.executor_factory(max_workers=2) as w:
            list(w.map(self.delete_environment, resources))


@resources.register('batch-definition')
class JobDefinition(QueryResourceManager):

    class resource_type(object):
        service = 'batch'
        filter_name = 'jobDefinitions'
        filter_type = 'list'
        dimension = None
        id = name = "jobDefinitionName"
        enum_spec = (
            'describe_job_definitions', 'jobDefinitions', None)
