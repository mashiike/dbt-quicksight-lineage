import pytest
import logging
import json
import os
from dbt_quicksight_lineage.core.quicksight import DataSet
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

@pytest.fixture
def source_data_set_dict(scope='class'):
    with open('tests/data/fixture/data_set.json', 'r', encoding='utf-8') as f:
        return json.load(f)

def assert_json_golden(golden_path, actual):
    if os.environ.get('PYTEST_UPDATE_GOLDEN'):
        with open(golden_path, 'w', encoding='utf-8') as f:
            json.dump(actual, f, indent=2, ensure_ascii=False)

    with open(golden_path, 'r', encoding='utf-8') as f:
        golden = json.load(f)
    assert actual == golden

class TestDataSet:
    physical_table_id:str = '12345678-9abc-def0-1234-56789abcdef0'

    def test_no_modify(self, source_data_set_dict):
        assert source_data_set_dict == DataSet(source_data_set_dict).to_dict()

    def test_set_rename_operation_not_exits(self, source_data_set_dict):
        data_set = DataSet(source_data_set_dict)
        data_set.set_rename_column_operation(
            physical_table_id=self.physical_table_id,
            physical_column_name='geo',
            logical_column_name='Geometry',
        )
        assert_json_golden(
            "tests/data/fixture/test_set_rename_column_oeration_not_exists.golden.json",
            data_set.to_dict(),
        )

    def test_set_rename_operation_exits_replace(self, source_data_set_dict):
        data_set = DataSet(source_data_set_dict)
        data_set.set_rename_column_operation(
            physical_table_id=self.physical_table_id,
            physical_column_name='id',
            logical_column_name='RowId',
        )
        assert_json_golden(
            "tests/data/fixture/test_set_rename_operation_exits_replace.golden.json",
            data_set.to_dict(),
        )

    def test_set_tag_column_operation_description_not_exists(self, source_data_set_dict):
        data_set = DataSet(source_data_set_dict)
        data_set.set_tag_column_description_operation(
            physical_table_id=self.physical_table_id,
            physical_column_name='geo',
            description='Geometory of city',
        )
        assert_json_golden(
            "tests/data/fixture/test_set_tag_column_operation_description_not_exists.golden.json",
            data_set.to_dict(),
        )

    def test_set_tag_column_operation_description_exists_replace(self, source_data_set_dict):
        data_set = DataSet(source_data_set_dict)
        data_set.set_tag_column_description_operation(
            physical_table_id=self.physical_table_id,
            physical_column_name='id',
            description='Row ID of this data set. it is PrimaryKey',
        )
        assert_json_golden(
            "tests/data/fixture/test_set_tag_column_operation_description_exists_replace.golden.json",
            data_set.to_dict(),
        )

    def test_set_tag_column_geographic_role_operation_not_exists(self, source_data_set_dict):
        data_set = DataSet(source_data_set_dict)
        data_set.set_tag_column_geographic_role_operation(
            physical_table_id=self.physical_table_id,
            physical_column_name='latitude',
            geographic_role='Latitude',
        )
        assert_json_golden(
            "tests/data/fixture/test_set_tag_column_geographic_role_operation_not_exists.golden.json",
            data_set.to_dict(),
        )

    def test_set_tag_column_geographic_role_operation_exists_replace(self, source_data_set_dict):
        data_set = DataSet(source_data_set_dict)
        data_set.set_tag_column_geographic_role_operation(
            physical_table_id=self.physical_table_id,
            physical_column_name='geo',
            geographic_role='City',
        )
        assert_json_golden(
            "tests/data/fixture/test_set_tag_column_geographic_role_operation_exists_replace.golden.json",
            data_set.to_dict(),
        )

    def test_set_cast_column_type_operation_not_exists(self, source_data_set_dict):
        data_set = DataSet(source_data_set_dict)
        data_set.set_cast_column_type_operation(
            physical_table_id=self.physical_table_id,
            physical_column_name='updated_at',
            column_type='STRING',
        )
        assert_json_golden(
            "tests/data/fixture/test_set_cast_column_type_operation_not_exists.golden.json",
            data_set.to_dict(),
        )

    def test_set_cast_column_type_operation_same_type(self, source_data_set_dict):
        data_set = DataSet(source_data_set_dict)
        data_set.set_cast_column_type_operation(
            physical_table_id=self.physical_table_id,
            physical_column_name='rate',
            column_type='DECIMAL',
        )
        assert_json_golden(
            "tests/data/fixture/test_set_cast_column_type_operation_same_type.golden.json",
            data_set.to_dict(),
        )

    def test_set_cast_column_type_operation_exists_replace(self, source_data_set_dict):
        data_set = DataSet(source_data_set_dict)
        data_set.set_cast_column_type_operation(
            physical_table_id=self.physical_table_id,
            physical_column_name='rate',
            column_type='INTEGER',
        )
        assert_json_golden(
            "tests/data/fixture/test_set_cast_column_type_operation_exists_replace.golden.json",
            data_set.to_dict(),
        )

    def test_add_to_projected_columns_not_exists(self, source_data_set_dict):
        data_set = DataSet(source_data_set_dict)
        data_set.add_to_projected_columns(
            physical_table_id=self.physical_table_id,
            physical_column_name='rate',
        )
        assert_json_golden(
            "tests/data/fixture/test_add_to_projected_columns_not_exists.golden.json",
            data_set.to_dict(),
        )

    def test_add_to_projected_columns_exists(self, source_data_set_dict):
        data_set = DataSet(source_data_set_dict)
        data_set.add_to_projected_columns(
            physical_table_id=self.physical_table_id,
            physical_column_name='id',
        )
        assert_json_golden(
            "tests/data/fixture/test_add_to_projected_columns_exists.golden.json",
            data_set.to_dict(),
        )

    def test_add_to_field_folder_not_exits(self, source_data_set_dict):
        data_set = DataSet(source_data_set_dict)
        data_set.add_to_field_folder(
            physical_table_id=self.physical_table_id,
            physical_column_name='geo',
            field_folder_path='Dimensions',
        )
        assert_json_golden(
            "tests/data/fixture/test_add_to_field_folder_not_exits.golden.json",
            data_set.to_dict(),
        )

    def test_add_to_field_folder_move(self, source_data_set_dict):
        data_set = DataSet(source_data_set_dict)
        data_set.add_to_field_folder(
            physical_table_id=self.physical_table_id,
            physical_column_name='id',
            field_folder_path='Dimensions',
        )
        assert_json_golden(
            "tests/data/fixture/test_add_to_field_folder_move.golden.json",
            data_set.to_dict(),
        )

    def test_add_to_field_folder_same_folder(self, source_data_set_dict):
        data_set = DataSet(source_data_set_dict)
        data_set.add_to_field_folder(
            physical_table_id=self.physical_table_id,
            physical_column_name='geo',
            field_folder_path='Key',
        )
        assert_json_golden(
            "tests/data/fixture/test_add_to_field_folder_same_folder.golden.json",
            data_set.to_dict(),
        )
