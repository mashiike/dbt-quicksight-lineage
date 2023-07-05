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
        """登録済みかどうか"""
        return self._physical_table['RelationalTable'] is not None

    @property
    def relational_table(self) -> Dict[str, Any]:
        """リレーショナルテーブル"""
        return self._physical_table['RelationalTable']


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

    def set_rename_column_operation(
        self,
        physical_column_name: str,
        logical_column_name: str
    ) -> None:
        """RenameColumnOperationを設定します"""
        exits = False
        last_rename_column_index = 0
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
        last_tag_column_index = 0
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
                operation['ProjectOperation']['ProjectedColumns'].append(target_column_name)


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
                self._data_set['FieldFolders'][k] = v.to_dict()

        if len(self._data_set['FieldFolders']) == 0:
            del self._data_set['FieldFolders']

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
            if physical_table.is_relational_table():
                yield physical_table
