import pytest
from dbt_quicksight_lineage.core import (
    ManifestLoader,
)


class TestManifestLoader:
    def test_load_from_json(self):
        loader = ManifestLoader(
            manifest_path='tests/data/manifest.json',
        )
        manifest = loader.load_manifest()
        actual = set([unique_id for unique_id in manifest.nodes.keys()])
        expected = set([
            'model.test_project.my_first_dbt_model',
            'model.test_project.my_second_dbt_model',
            'test.test_project.unique_my_second_dbt_model_id.57a0f8c493',
            'test.test_project.not_null_my_second_dbt_model_id.151b76d778',
            'test.test_project.unique_my_first_dbt_model_id.16e066b321',
            'test.test_project.not_null_my_first_dbt_model_id.5fb22c2710',
        ])
        assert actual == expected
        acutal_meta = manifest.nodes['model.test_project.my_first_dbt_model'].meta
        expected_meta = {
            'quicksight': {
                'data_sets': [
                    {
                        'id': '00000000-0000-0000-0000-000000000000',
                        'data_source': 'arn:aws:quicksight:ap-northeast-1:123456789012:datasource/00000000-0000-0000-0000-000000000000',
                    },
                    {
                        'id': '11111111-1111-1111-1111-111111111111',
                        'data_source': 'arn:aws:quicksight:ap-northeast-1:123456789012:datasource/11111111-1111-1111-1111-111111111111',
                    },
                ],
                'logical_table_name': 'My First DBT Model',
            }
        }
        assert acutal_meta == expected_meta
        acutal_column_meta = manifest.nodes['model.test_project.my_first_dbt_model'].columns['id'].meta
        expected_column_meta = {
            'quicksight': {
                'field_name': 'ID',
                'folder': 'Key/',
            },
        }
        assert acutal_column_meta == expected_column_meta

    def test_load_from_msgpack(self):
        loader = ManifestLoader(
            manifest_path='tests/data/partial_parse.msgpack',
        )
        manifest = loader.load_manifest()
        actual = set([unique_id for unique_id in manifest.nodes.keys()])
        expected = set([
            'model.test_project.my_first_dbt_model',
            'model.test_project.my_second_dbt_model',
            'test.test_project.unique_my_second_dbt_model_id.57a0f8c493',
            'test.test_project.not_null_my_second_dbt_model_id.151b76d778',
            'test.test_project.unique_my_first_dbt_model_id.16e066b321',
            'test.test_project.not_null_my_first_dbt_model_id.5fb22c2710',
        ])
        assert actual == expected
        acutal_meta = manifest.nodes['model.test_project.my_first_dbt_model'].meta
        expected_meta = {
            'quicksight': {
                'data_sets': [
                    {
                        'id': '00000000-0000-0000-0000-000000000000',
                        'data_source': 'arn:aws:quicksight:ap-northeast-1:123456789012:datasource/00000000-0000-0000-0000-000000000000',
                    },
                    {
                        'id': '11111111-1111-1111-1111-111111111111',
                        'data_source': 'arn:aws:quicksight:ap-northeast-1:123456789012:datasource/11111111-1111-1111-1111-111111111111',
                    },
                ],
                'logical_table_name': 'My First DBT Model',
            }
        }
        assert acutal_meta == expected_meta
        acutal_column_meta = manifest.nodes['model.test_project.my_first_dbt_model'].columns['id'].meta
        expected_column_meta = {
            'quicksight': {
                'field_name': 'ID',
                'folder': 'Key/',
            },
        }
        assert acutal_column_meta == expected_column_meta

    def test_load_from_project(self):
        loader = ManifestLoader(
            project_dir='tests/data/test_project',
            profiles_dir='tests/data/test_project',
            target='dev',
        )
        manifest = loader.load_manifest()
        actual = set([unique_id for unique_id in manifest.nodes.keys()])
        expected = set([
            'model.test_project.my_first_dbt_model',
            'model.test_project.my_second_dbt_model',
            'test.test_project.unique_my_second_dbt_model_id.57a0f8c493',
            'test.test_project.not_null_my_second_dbt_model_id.151b76d778',
            'test.test_project.unique_my_first_dbt_model_id.16e066b321',
            'test.test_project.not_null_my_first_dbt_model_id.5fb22c2710',
        ])
        assert actual == expected
        acutal_meta = manifest.nodes['model.test_project.my_first_dbt_model'].meta
        expected_meta = {
            'quicksight': {
                'data_sets': [
                    {
                        'id': '00000000-0000-0000-0000-000000000000',
                        'data_source': 'arn:aws:quicksight:ap-northeast-1:123456789012:datasource/00000000-0000-0000-0000-000000000000',
                    },
                    {
                        'id': '11111111-1111-1111-1111-111111111111',
                        'data_source': 'arn:aws:quicksight:ap-northeast-1:123456789012:datasource/11111111-1111-1111-1111-111111111111',
                    },
                ],
                'logical_table_name': 'My First DBT Model',
            }
        }
        assert acutal_meta == expected_meta
        acutal_column_meta = manifest.nodes['model.test_project.my_first_dbt_model'].columns['id'].meta
        expected_column_meta = {
            'quicksight': {
                'field_name': 'ID',
                'folder': 'Key/',
            },
        }
        assert acutal_column_meta == expected_column_meta

    def test_load_invalid(self):
        loader = ManifestLoader('tests/data/invalid')
        with pytest.raises(ValueError):
            loader.load_manifest()
