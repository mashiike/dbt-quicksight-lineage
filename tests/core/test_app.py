import pytest
import os
import boto3
import botocore
import json
import shutil
import filecmp
from moto import mock_quicksight, mock_sts
from mock import patch
from dbt_quicksight_lineage.core import (
    ManifestLoader,
    App,
    DataSet,
)
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


@pytest.fixture(scope='class')
def example_manifest():
    loader = ManifestLoader(
        manifest_path='tests/data/manifest.json',
    )
    return loader.load_manifest()


def set_env():
    os.environ['AWS_ACCESS_KEY_ID'] = 'test'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'test'
    os.environ['AWS_DEFAULT_REGION'] = 'ap-northeast-1'
    os.environ['AWS_SECURITY_TOKEN'] = 'test'
    os.environ['AWS_SESSION_TOKEN'] = 'test'


@pytest.fixture
def mock_quicksight_client():
    set_env()

    with mock_quicksight():
        client = boto3.client('quicksight')
        return client


orig = botocore.client.BaseClient._make_api_call


def mock_make_api_call(self, operation_name, kwarg):
    if operation_name == 'DescribeDataSet':
        with open('tests/data/describe_data_set_output.json', 'r') as f:
            describe_data_set_output = json.load(f)
        return describe_data_set_output
    return orig(self, operation_name, kwarg)


@mock_sts
class TestApp:
    def test_find_models_by_data_set(self, example_manifest, mock_quicksight_client):
        app = App(
            quicksight_client=mock_quicksight_client,
            manifest=example_manifest
        )
        actual = set([
            model.unique_id
            for model in app._find_models_by_data_set(
                '00000000-0000-0000-0000-000000000000',
                'arn:aws:quicksight:ap-northeast-1:123456789012:datasource/00000000-0000-0000-0000-000000000000'
            )
        ])
        expected = set([
            'model.test_project.my_first_dbt_model',
        ])
        assert actual == expected

    def test__detect_modify_target(self, example_manifest, mock_quicksight_client):
        app = App(
            quicksight_client=mock_quicksight_client,
            manifest=example_manifest
        )
        with open('tests/data/describe_data_set_output.json', 'r') as f:
            describe_data_set_output = json.load(f)
        actual = set([
            (physical_table_id, node.unique_id)
            for physical_table_id, node in app._detect_modify_target(DataSet(describe_data_set_output.get('DataSet')))
        ])
        expected = set([
            ('12345678-9abc-def0-1234-56789abcdef0',
             'model.test_project.my_first_dbt_model'),
        ])
        assert actual == expected

    def test_generate_logical_table(self, example_manifest, mock_quicksight_client):
        app = App(
            quicksight_client=mock_quicksight_client,
            manifest=example_manifest
        )
        with open('tests/data/describe_data_set_output.json', 'r') as f:
            describe_data_set_output = json.load(f)
        data_set = DataSet(describe_data_set_output.get('DataSet'))
        physical_table_id = '12345678-9abc-def0-1234-56789abcdef0'
        logical_table, filed_folders = app._generate_logical_table(
            physical_table_id,
            data_set.physical_relational_table(physical_table_id),
            example_manifest.nodes['model.test_project.my_first_dbt_model'],
        )

        with open('tests/data/modified_data_set.json') as f:
            modified_data_set = DataSet(json.load(f))
        _, expected = modified_data_set.find_logical_table_by_physical_table_id(
            physical_table_id)
        assert expected == logical_table
        assert modified_data_set.filed_folders() == filed_folders

    @patch('botocore.client.BaseClient._make_api_call', new=mock_make_api_call)
    def test_update_data_set_dry_run(
        self,
        example_manifest,
        mock_quicksight_client,
    ):
        app = App(
            quicksight_client=mock_quicksight_client,
            manifest=example_manifest
        )
        _, input = app.update_data_set(
            '00000000-0000-0000-0000-000000000000',
            dry_run=True,
        )
        with open('tests/data/modified_data_set.json') as f:
            modified_data_set = DataSet(json.load(f))
        assert input == modified_data_set.generate_update_data_set_input(
            '123456789012')

    def test_detect_related_nodes(self, example_manifest, mock_quicksight_client):
        app = App(
            quicksight_client=mock_quicksight_client,
            manifest=example_manifest
        )
        with open('tests/data/modified_data_set.json', 'r') as f:
            data_set = DataSet(json.load(f))

        actual = set([
            (physical_table_id, node.unique_id)
            for physical_table_id, node in app._detect_related_nodes(
                data_set,
            )
        ])
        expected = set([
            ('12345678-9abc-def0-1234-56789abcdef0',
             'model.test_project.my_first_dbt_model'),
        ])
        assert actual == expected

    def test_generate_schema_dict(self, example_manifest, mock_quicksight_client):
        app = App(
            quicksight_client=mock_quicksight_client,
            manifest=example_manifest
        )
        with open('tests/data/modified_data_set.json', 'r') as f:
            data_set = DataSet(json.load(f))
        actual = app._generate_schema_dict(
            'test_model',
            data_set,
            '12345678-9abc-def0-1234-56789abcdef0',
        )
        expected = {
            'models': [
                {
                    'name': 'test_model',
                    'meta': {
                        'quicksight': {
                            'logical_table_name': 'My First DBT Model',
                            'data_sets': [
                                {
                                    'id': '00000000-0000-0000-0000-000000000000',
                                    'data_source_arn': 'arn:aws:quicksight:ap-northeast-1:123456789012:datasource/00000000-0000-0000-0000-000000000000',
                                },
                            ],
                        },
                    },
                    'columns': [
                        {
                            'name': 'id',
                            'description': 'The primary key for this table',
                            'meta': {
                                'quicksight': {
                                    'field_name': 'ID',
                                    'folder': 'Key',
                                },
                            },
                        },
                    ],
                },
            ],
        }
        assert actual == expected

    def test_update_schema_yaml(self, example_manifest, mock_quicksight_client):
        app = App(
            quicksight_client=mock_quicksight_client,
            manifest=example_manifest
        )
        node = example_manifest.nodes['model.test_project.my_first_dbt_model']
        with open('tests/data/modified_data_set.json', 'r') as f:
            data_set = DataSet(json.load(f))
        shutil.copyfile(
            'tests/data/schema.yml',
            'tests/data/models/example/schema.yml',
        )
        app._update_schema_yaml(
            data_set,
            '12345678-9abc-def0-1234-56789abcdef0',
            node,
            'tests/data/',
        )
        assert filecmp.cmp('tests/data/models/example/schema.yml',
                           'tests/data/expected_schema.yml')
