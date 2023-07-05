"""このモジュールはQuickSightのDataSetの操作に関するモジュールです。"""
import json
from typing import Any, Dict, Optional, Iterator


class PhysicalTable:
    """
    DataSetの物理テーブルを表現するクラスです。
    DataSetの物理テーブルのアクセスインタフェースを提供します
    """

    def __init__(self, physical_table_id: str, physical_table: Dict[str, Any]) -> None:
        self._physical_table_id = physical_table_id
        self._physical_table = physical_table

    def get(self, key: str, default: Optional[Any] = None) -> Any:
        """属性を取得します"""
        return self._physical_table.get(key, default)

    def __getitem__(self, key: str) -> Any:
        return self._physical_table[key]

    def to_dict(self) -> Dict[str, Any]:
        """DataSetの属性を辞書に変換します"""
        return self._physical_table

    @property
    def physical_table_id(self) -> str:
        """物理テーブルID"""
        return self._physical_table_id

    @property
    def is_relational_table(self) -> bool:
        """リレーショナルテーブルですか？"""
        return self.relational_table is not None

    @property
    def relational_table(self) -> Optional[Dict[str, Any]]:
        """リレーショナルテーブル"""
        return self._physical_table.get('RelationalTable')

    @property
    def data_source_arn(self) -> Optional[str]:
        """リレーショナルテーブルのデータソースARN"""
        if self.is_relational_table:
            return self.relational_table.get('DataSourceArn')
        return None

    @property
    def schema_name(self) -> Optional[str]:
        """リレーショナルテーブルのスキーマ名"""
        if self.is_relational_table:
            return self.relational_table.get('Schema')
        return None

    @property
    def table_name(self) -> Optional[str]:
        """リレーショナルテーブルのテーブル名"""
        if self.is_relational_table:
            return self.relational_table.get('Name')
        return None

    @property
    def columns(self) -> Iterator[Dict[str, Any]]:
        """リレーショナルテーブルのカラム"""
        if self.is_relational_table:
            return iter(self.relational_table.get('InputColumns', []))
        return iter([])

    def get_column_type(
        self,
        column_name: str,
    ) -> str:
        """カラムの型を取得します"""
        for column in self.columns:
            if column['Name'] == column_name:
                return column['Type']
        raise KeyError(f'column_name: {column_name} is not found')

class LogicalTable:
    """
    DataSetの論理テーブルを表現するクラスです。
    """

    def __init__(self, logical_table_id: str, logical_table: Dict[str, Any]) -> None:
        self._logical_table_id = logical_table_id
        self._logical_table = logical_table

    def get(self, key: str, default: Optional[Any] = None) -> Any:
        """属性を取得します"""
        return self._logical_table.get(key, default)

    def __getitem__(self, key: str) -> Any:
        return self._logical_table[key]

    def to_dict(self) -> Dict[str, Any]:
        """属性を辞書に変換します"""
        return self._logical_table

    @property
    def logical_table_id(self) -> str:
        """論理テーブルID"""
        return self._logical_table_id

    @property
    def related_physical_table_id(self) -> Optional[str]:
        """関連する物理テーブルID"""
        return self._logical_table.get('Source', {}).get('PhysicalTableId')

    def _before_project_operation_index(self) -> int:
        """ProjectOperationのインデックスを取得します"""
        for index, operation in enumerate(self._logical_table['DataTransforms']):
            if operation.get('ProjectOperation') is not None:
                return index - 1
        return 0

    def set_rename_column_operation(
        self,
        physical_column_name: str,
        logical_column_name: str
    ) -> None:
        """RenameColumnOperationを設定します"""
        exits = False
        last_rename_column_index = self._before_project_operation_index()
        old_column_name = physical_column_name
        for index, operation in enumerate(self._logical_table['DataTransforms']):
            if operation.get('RenameColumnOperation') is not None:
                last_rename_column_index = index
                if operation['RenameColumnOperation']['ColumnName'] == physical_column_name:
                    exits = True
                    old_column_name = operation['RenameColumnOperation']['NewColumnName']
                    operation['RenameColumnOperation']['NewColumnName'] = logical_column_name
                    break
        if not exits:
            self._logical_table['DataTransforms'].insert(
                last_rename_column_index + 1,
                {
                    'RenameColumnOperation': {
                        'ColumnName': physical_column_name,
                        'NewColumnName': logical_column_name
                    }
                },
            )
        for index, operation in enumerate(self._logical_table['DataTransforms']):
            for key in operation.keys():
                if key != 'RenameColumnOperation':
                    if operation[key].get('ColumnName') in [
                        physical_column_name,
                        old_column_name,
                    ]:
                        operation[key]['ColumnName'] = logical_column_name
                if key == 'ProjectOperation':
                    for j, column_name in enumerate(operation[key]['ProjectedColumns']):
                        if column_name in [
                            physical_column_name,
                            old_column_name,
                        ]:
                            operation[key]['ProjectedColumns'][j] = logical_column_name

    def get_logical_column_name(
        self,
        physical_column_name: str
    ) -> Optional[str]:
        """
        物理カラム名から論理カラム名を取得します。
        論理カラム名が設定されてない場合は、Noneが返ります。
        論理カラム名はRenameColoumnOperationによって設定されたカラム名です。
        """
        for operation in self._logical_table['DataTransforms']:
            if operation.get('RenameColumnOperation') is not None:
                if operation['RenameColumnOperation']['ColumnName'] == physical_column_name:
                    return operation['RenameColumnOperation']['NewColumnName']
        return None

    def get_output_column_name(
        self,
        physical_column_name: str
    ) -> str:
        """物理カラム名から出力としてのカラム名を取得します。"""
        return self.get_logical_column_name(
            physical_column_name,
        ) or physical_column_name

    def get_tag_column_description(
        self,
        physical_column_name: str
    ) -> Optional[str]:
        """
        物理カラム名から説明のタグを取得します
        """
        target_column_name = self.get_output_column_name(physical_column_name)
        for operation in self._logical_table['DataTransforms']:
            if operation.get('TagColumnOperation') is not None:
                if operation['TagColumnOperation']['ColumnName'] == target_column_name:
                    for tag in operation['TagColumnOperation']['Tags']:
                        if tag.get('ColumnDescription') is not None:
                            return tag['ColumnDescription']['Text']
        return None

    def set_tag_column_description_operation(
        self,
        physical_column_name: str,
        description: str
    ) -> None:
        """
        TagColumnOperationのColumnDescriptionを設定します
        """
        target_column_name = self.get_output_column_name(physical_column_name)
        exits = False
        last_tag_column_index = self._before_project_operation_index()
        for index, operation in enumerate(self._logical_table['DataTransforms']):
            if operation.get('TagColumnOperation') is not None:
                last_tag_column_index = index
                if operation['TagColumnOperation']['ColumnName'] != target_column_name:
                    continue
                for j, tag in enumerate(operation['TagColumnOperation']['Tags']):
                    if tag.get('ColumnDescription') is not None:
                        exits = True
                        tag['ColumnDescription']['Text'] = description
                        operation['TagColumnOperation']['Tags'][j] = tag
                        break
                if exits:
                    break
        if not exits:
            self._logical_table['DataTransforms'].insert(
                last_tag_column_index + 1,
                {
                    'TagColumnOperation': {
                        'ColumnName': target_column_name,
                        'Tags': [
                            {
                                'ColumnDescription': {
                                    'Text': description
                                }
                            }
                        ]
                    }
                },
            )

    def remove_cast_column_type_operation(
        self,
        physical_column_name: str
    ) -> None:
        """
        CastColumnTypeOperationを削除します
        """
        target_column_name = self.get_output_column_name(physical_column_name)
        for index, operation in enumerate(self._logical_table['DataTransforms']):
            if operation.get('CastColumnTypeOperation') is not None:
                if operation['CastColumnTypeOperation']['ColumnName'] == target_column_name:
                    del self._logical_table['DataTransforms'][index]
                    break

    def get_cast_column_type(
        self,
        physical_column_name: str
    ) -> Optional[str]:
        """
        物理カラム名からCastColumnTypeOperationのNewColumnTypeを取得します
        """
        target_column_name = self.get_output_column_name(physical_column_name)
        for operation in self._logical_table['DataTransforms']:
            if operation.get('CastColumnTypeOperation') is not None:
                if operation['CastColumnTypeOperation']['ColumnName'] == target_column_name:
                    return operation['CastColumnTypeOperation']['NewColumnType']
        return None

    def set_cast_column_type_operation(
        self,
        physical_column_name: str,
        column_type: str,
    ) -> None:
        """
        CastColumnTypeOperationを設定します
        """
        target_column_name = self.get_output_column_name(physical_column_name)
        exits = False
        last_cast_column_type_index = self._before_project_operation_index()
        for index, operation in enumerate(self._logical_table['DataTransforms']):
            if operation.get('CastColumnTypeOperation') is not None:
                last_cast_column_type_index = index
                if operation['CastColumnTypeOperation']['ColumnName'] == target_column_name:
                    exits = True
                    operation['CastColumnTypeOperation']['NewColumnType'] = column_type.upper()
                    break
        if not exits:
            self._logical_table['DataTransforms'].insert(
                last_cast_column_type_index + 1,
                {
                    'CastColumnTypeOperation': {
                        'ColumnName': target_column_name,
                        'NewColumnType': column_type.upper()
                    }
                },
            )

    def get_tag_column_geographic_role(
        self,
        physical_column_name: str
    ) -> Optional[str]:
        """
        物理カラム名から地理情報のタグを取得します
        """
        target_column_name = self.get_output_column_name(physical_column_name)
        for operation in self._logical_table['DataTransforms']:
            if operation.get('TagColumnOperation') is not None:
                if operation['TagColumnOperation']['ColumnName'] == target_column_name:
                    for tag in operation['TagColumnOperation']['Tags']:
                        if tag.get('ColumnGeographicRole') is not None:
                            return tag['ColumnGeographicRole']
        return None

    def set_tag_column_geographic_role_operation(
        self,
        physical_column_name: str,
        geographic_role: str
    ) -> None:
        """
        TagColumnOperationのColumnGeographicRoleを設定します
        """
        target_column_name = self.get_output_column_name(physical_column_name)
        exits = False
        last_tag_column_index = self._before_project_operation_index()
        for index, operation in enumerate(self._logical_table['DataTransforms']):
            if operation.get('TagColumnOperation') is not None:
                last_tag_column_index = index
                if operation['TagColumnOperation']['ColumnName'] != target_column_name:
                    continue
                for j, tag in enumerate(operation['TagColumnOperation']['Tags']):
                    if tag.get('ColumnGeographicRole') is not None:
                        exits = True
                        tag['ColumnGeographicRole'] = geographic_role.upper()
                        operation['TagColumnOperation']['Tags'][j] = tag
                        break
                if exits:
                    break
        if not exits:
            self._logical_table['DataTransforms'].insert(
                last_tag_column_index + 1,
                {
                    'TagColumnOperation': {
                        'ColumnName': target_column_name,
                        'Tags': [
                            {
                                'ColumnGeographicRole': geographic_role.upper()
                            }
                        ]
                    }
                },
            )

    def add_to_projected_columns(
        self,
        physical_column_name: str
    ) -> None:
        """
        ProjectedColumnsにカラムを追加します
        すでにあったら何もしません。
        なかったら追加します。
        """
        target_column_name = self.get_output_column_name(physical_column_name)
        for operation in self._logical_table['DataTransforms']:
            if operation.get('ProjectOperation') is not None:
                if target_column_name in operation['ProjectOperation']['ProjectedColumns']:
                    return
                operation['ProjectOperation']['ProjectedColumns'].append(
                    target_column_name)
                return
        self._logical_table['DataTransforms'].append(
            {
                'ProjectOperation': {
                    'ProjectedColumns': [
                        target_column_name
                    ]
                }
            }
        )

    def contains_projected_columns(
        self,
        physical_column_name: str
    ) -> bool:
        """
        ProjectedColumnsにカラムが含まれているかどうかを返します
        """
        target_column_name = self.get_output_column_name(physical_column_name)
        for operation in self._logical_table['DataTransforms']:
            if operation.get('ProjectOperation') is not None:
                if target_column_name in operation['ProjectOperation']['ProjectedColumns']:
                    return True
        return False

    def remove_from_projected_columns(
        self,
        physical_column_name: str
    ) -> None:
        """
        ProjectedColumnsからカラムを削除します
        """
        target_column_name = self.get_output_column_name(physical_column_name)
        for operation in self._logical_table['DataTransforms']:
            if operation.get('ProjectOperation') is not None:
                if target_column_name in operation['ProjectOperation']['ProjectedColumns']:
                    operation['ProjectOperation']['ProjectedColumns'].remove(
                        target_column_name)
                    if len(operation['ProjectOperation']['ProjectedColumns']) == 0:
                        self._logical_table['DataTransforms'].remove(operation)
                    return

    def set_alias(
        self,
        alias_name: str
    ) -> None:
        """
        Aliasを設定します
        """
        self._logical_table['Alias'] = alias_name


class FieldFolder:
    """
    DataSetのフィールドフォルダを表現するクラスです。
    """

    def __init__(self, field_folder_path: str, field_folders: Optional[Dict[str, Any]]) -> None:
        self._field_folder_path = field_folder_path
        self.description = field_folders.get('description')
        self.columns = field_folders.get('columns', [])

    @property
    def field_folder_path(self) -> str:
        """フィールドフォルダパス"""
        return self._field_folder_path

    def to_dict(self) -> Dict[str, Any]:
        """属性を辞書に変換します"""
        result = {}
        if self.description is not None:
            result['description'] = self.description
        if self.columns is not None:
            result['columns'] = self.columns
        return result

    def contains_column(
            self,
            column_name: str
    ) -> bool:
        """指定したカラム名を含むかどうかを返します"""
        return column_name in self.columns

    def remove_column(
            self,
            column_name: str
    ) -> None:
        """指定したカラム名を削除します"""
        self.columns.remove(column_name)

    def add_column(
            self,
            column_name: str
    ) -> None:
        """指定したカラム名を追加します"""
        self.columns.append(column_name)

    @property
    def column_count(self) -> int:
        """カラム数"""
        return len(self.columns)


class DataSet:
    """
    QuickSightのDataSetを表現するクラスです。
    DataSetの操作インタフェースを提供します
    """

    def __init__(self, data_set: Dict[str, Any]) -> None:
        self._data_set = data_set
        self._physical_table_map = {
            k: PhysicalTable(k, v)
            for k, v in self._data_set['PhysicalTableMap'].items()
        }
        self._logical_tabel_map = {
            k: LogicalTable(k, v)
            for k, v in self._data_set['LogicalTableMap'].items()
        }
        self._field_folders = {
            k: FieldFolder(k, v)
            for k, v in self._data_set['FieldFolders'].items()
        }

    def get(self, key: str, default: Optional[Any] = None) -> Any:
        """DataSetの属性を取得します"""
        return self._data_set.get(key, default)

    def __getitem__(self, key: str) -> Any:
        return self._data_set[key]

    def to_dict(self) -> Dict[str, Any]:
        """DataSetの属性を辞書に変換します"""
        self._data_set['PhysicalTableMap'] = {
            k: v.to_dict()
            for k, v in self._physical_table_map.items()
        }
        self._data_set['LogicalTableMap'] = {
            k: v.to_dict()
            for k, v in self._logical_tabel_map.items()
        }
        self._data_set['FieldFolders'] = {}
        for k, v in self._field_folders.items():
            if v.column_count > 0:
                self._data_set['FieldFolders'][k.rstrip('/')] = v.to_dict()

        if len(self._data_set['FieldFolders']) == 0:
            del self._data_set['FieldFolders']
        if 'OutputColumns' in self._data_set:
            del self._data_set['OutputColumns']
        if 'LastUpdatedTime' in self._data_set:
            del self._data_set['LastUpdatedTime']
        return self._data_set

    def to_json(self) -> str:
        """to_json returns JSON string of DataSet"""
        return json.dumps(self.to_dict(), indent=2, default=str)

    @property
    def data_set_id(self) -> str:
        """DataSet ID"""
        return self._data_set['DataSetId']

    @property
    def name(self) -> str:
        """DataSet名"""
        return self._data_set['Name']

    @property
    def physical_table_map(self) -> Dict[str, PhysicalTable]:
        """物理テーブルのマップ"""
        return self._physical_table_map

    @property
    def logical_table_map(self) -> Dict[str, LogicalTable]:
        """論理テーブルのマップ"""
        return self._logical_tabel_map

    @property
    def field_folders(self) -> Dict[str, FieldFolder]:
        """フィールドフォルダのマップ"""
        return self._field_folders

    def find_logical_by_physical(
        self,
        physical_table_id: str
    ) -> Iterator[LogicalTable]:
        """
        物理テーブルIDから論理テーブルを検索します
        """
        for logical_table in self._logical_tabel_map.values():
            if logical_table.related_physical_table_id != physical_table_id:
                continue
            yield logical_table

    def set_rename_column_operation(
            self,
            physical_table_id: str,
            physical_column_name: str,
            logical_column_name: str,
    ) -> None:
        """
        指定された物理テーブルの指定された物理カラム名を指定された論理カラム名に変更する
        RenameColumnOperationを設定します
        """
        for logical_table in self.find_logical_by_physical(physical_table_id):
            logical_table.set_rename_column_operation(
                physical_column_name, logical_column_name
            )

    def set_tag_column_description_operation(
            self,
            physical_table_id: str,
            physical_column_name: str,
            description: str
    ) -> None:
        """
        指定された物理テーブルの指定された物理カラムのColumnDescriptionを設定します
        """
        for logical_table in self.find_logical_by_physical(physical_table_id):
            logical_table.set_tag_column_description_operation(
                physical_column_name, description
            )

    def set_tag_column_geographic_role_operation(
        self,
        physical_table_id: str,
        physical_column_name: str,
        geographic_role: str
    ) -> None:
        """
        指定された物理テーブルの指定された物理カラムのGeographicRoleを設定します
        """
        for logical_table in self.find_logical_by_physical(physical_table_id):
            logical_table.set_tag_column_geographic_role_operation(
                physical_column_name, geographic_role
            )

    def remove_cast_column_type_operation(
        self,
        physical_table_id: str,
        physical_column_name: str
    ) -> None:
        """
        指定された物理テーブルの指定された物理カラムのColumnTypeを削除します
        """
        for logical_table in self.find_logical_by_physical(physical_table_id):
            logical_table.remove_cast_column_type_operation(
                physical_column_name
            )

    def set_cast_column_type_operation(
        self,
        physical_table_id: str,
        physical_column_name: str,
        column_type: str
    ) -> None:
        """
        指定された物理テーブルの指定された物理カラムのColumnTypeを設定します
        """
        physical_table = self._physical_table_map[physical_table_id]
        physical_column_type = physical_table.get_column_type(physical_column_name)
        if physical_column_type.upper() == column_type.upper():
            self.remove_cast_column_type_operation(
                physical_table_id, physical_column_name
            )
            return
        for logical_table in self.find_logical_by_physical(physical_table_id):
            logical_table.set_cast_column_type_operation(
                physical_column_name, column_type
            )

    def add_to_projected_columns(
            self,
            physical_table_id: str,
            physical_column_name: str,
    ) -> None:
        """
        指定された物理テーブルの指定された物理カラムをProjectedColumnsに追加します
        """
        for logical_table in self.find_logical_by_physical(physical_table_id):
            logical_table.add_to_projected_columns(physical_column_name)

    def remove_from_projected_columns(
        self,
        physical_table_id: str,
        physical_column_name: str,
    ) -> None:
        """
        指定された物理テーブルの指定された物理カラムをProjectedColumnsから削除します
        """
        for logical_table in self.find_logical_by_physical(physical_table_id):
            logical_table.remove_from_projected_columns(physical_column_name)

    def add_to_field_folder(
            self,
            physical_table_id: str,
            physical_column_name: str,
            field_folder_path: str
    ) -> None:
        """
        指定された物理テーブルの指定された物理カラムを指定されたフィールドフォルダに追加します
        """
        if field_folder_path not in self._field_folders:
            self._field_folders[field_folder_path] = FieldFolder(
                field_folder_path, {}
            )
        self.add_to_projected_columns(physical_table_id, physical_column_name)
        for logical_table in self.find_logical_by_physical(physical_table_id):
            column_name = logical_table.get_output_column_name(
                physical_column_name
            )
            for field_folder in self._field_folders.values():
                if field_folder.field_folder_path != field_folder_path:
                    if field_folder.contains_column(column_name):
                        field_folder.remove_column(column_name)
                    continue
                field_folder.add_column(column_name)

    def find_relational_table(self) -> Iterator[PhysicalTable]:
        """
        リレーショナルテーブルな物理テーブルの情報を検索します。
        """
        for physical_table in self._physical_table_map.values():
            if physical_table.is_relational_table:
                yield physical_table

    def get_field_folder_path(
            self,
            physical_table_id: str,
            physical_column_name: str
    ) -> Optional[str]:
        """
        指定された物理テーブルの指定された物理カラムが属するフィールドフォルダのパスを取得します
        """
        for logical_table in self.find_logical_by_physical(physical_table_id):
            column_name = logical_table.get_output_column_name(
                physical_column_name
            )
            for field_folder in self._field_folders.values():
                if field_folder.contains_column(column_name):
                    return field_folder.field_folder_path
        return None

    def set_alias(
        self,
        physical_table_id: str,
        alias: str
    ) -> None:
        """
        指定された物理テーブルのAliasに関連した設定します
        """
        for logical_table in self.find_logical_by_physical(physical_table_id):
            logical_table.set_alias(alias)

    def add_field_folder(
        self,
        field_folder_path: str,
        description: Optional[str] = None,
    ) -> None:
        """
        指定されたフィールドフォルダを追加します
        """
        if field_folder_path not in self._field_folders:
            self._field_folders[field_folder_path] = FieldFolder(
                field_folder_path, {},
            )
        if description is not None:
            self._field_folders[field_folder_path].description = description

    _update_data_set_input_keys = [
        'AwsAccountId',
        'DataSetId',
        'Name',
        'PhysicalTableMap',
        'LogicalTableMap',
        'ImportMode',
        'ColumnGroups',
        'FieldFolders',
        'RowLevelPermissionDataSet',
        'RowLevelPermissionTagConfiguration',
        'ColumnLevelPermissionRules',
        'DataSetUsageConfiguration',
        'DatasetParameters',
    ]

    def generate_update_data_set_input(
        self,
        aws_account_id: str,
    ) -> Dict[str, Any]:
        """
        UpdateDataSetのInputを生成します
        """
        update_data_set_input = self.to_dict()
        update_data_set_input['AwsAccountId'] = aws_account_id
        keys = list(update_data_set_input.keys())
        for key in keys:
            if key not in self._update_data_set_input_keys:
                del update_data_set_input[key]
        return update_data_set_input
