"""dbt_quicksight_lineage.core.app is application core logic"""
import logging
import json
import uuid
import os
from typing import Iterator, Optional, Any, Dict, Tuple, List
import boto3
from ruamel import yaml
from dbt.contracts.graph.manifest import Manifest, ManifestNode
logger = logging.getLogger()


class DataSet:
    """DataSet is aws quicksight DataSet dict wrapper"""

    def __init__(
        self,
        data_set: Dict[str, Any],
    ) -> None:
        self.data_set = data_set

    def __getitem__(self, key: str) -> Any:
        return self.data_set[key]

    def data_set_id(self) -> str:
        """id returns DataSetId"""
        return self.data_set.get('DataSetId')

    def physical_table_map(self) -> Dict[str, Any]:
        """physical_table_map returns PhysicalTableMap"""
        return self.data_set.get('PhysicalTableMap', {})

    def physical_relational_table(self, physical_table_id: str) -> Dict[str, Any]:
        """physical_relational_table returns RelationalTable in PhysicalTableMa p"""
        return self.physical_table_map().get(physical_table_id, {}).get('RelationalTable', {})

    def logical_table_map(self) -> Dict[str, Any]:
        """logical_table_map returns LogicalTableMap"""
        return self.data_set.get('LogicalTableMap', {})

    def find_logical_table_by_physical_table_id(
        self,
        physical_table_id: str
    ) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        """find_logical_table_by_physical_table_id returns LogicalTableId and LogicalTable"""
        for logical_table_id, logical_table in self.logical_table_map().items():
            if logical_table.get('Source', {}).get('PhysicalTableId') == physical_table_id:
                return logical_table_id, logical_table
        return None, None

    def replace_logical_table(
            self,
            physical_table_id: str,
            logical_table: Dict[str, Any]
    ) -> None:
        """replace_logical_table replaces LogicalTable in LogicalTableMap"""
        logical_table_id, old_logical_table = self.find_logical_table_by_physical_table_id(
            physical_table_id)
        if logical_table_id is None:
            logical_table_id = str(uuid.uuid4())
            logger.info(
                "No logical table found for physical table %s. Creating new logical table %s.",
                physical_table_id,
                logical_table_id,
            )
            old_logical_table = {}
        else:
            logger.info("Updating logical table %s", logical_table_id)

        logger.debug("old logical table: %s", old_logical_table)
        logger.debug("new logical table: %s", logical_table)
        self.logical_table_map()[logical_table_id] = logical_table

    def filed_folders(self) -> Optional[Dict[str, Any]]:
        """filed_folders returns FieldFolders"""
        return self.data_set.get('FieldFolders')

    def replace_field_folder(self, physical_columns: List[str], new_field_folder: Dict[str, Any]):
        """replace_field_folder replaces FieldFolders"""
        field_folders = self.filed_folders() or {}
        field_folder_paths = list(field_folders.keys())
        for field_folder_path in field_folder_paths:
            columns = field_folders[field_folder_path].get('columns', [])
            for physical_column in physical_columns:
                if physical_column in columns:
                    columns.remove(physical_column)
            if len(columns) == 0:
                del field_folders[field_folder_path]
            else:
                field_folders[field_folder_path]['columns'] = columns

        for folder_path, folder in new_field_folder.items():
            if folder_path in field_folders:
                if folder.get('description'):
                    field_folders[folder_path]['description'] = folder['description']
                if folder.get('columns'):
                    columns = field_folders[folder_path]['columns']
                    for column in folder['columns']:
                        if column not in columns:
                            columns.append(column)
                    field_folders[folder_path]['columns'] = columns
            else:
                field_folders[folder_path] = folder
        self.data_set['FieldFolders'] = field_folders

    def generate_update_data_set_input(self, aws_account_id: str) -> Dict[str, Any]:
        """generate_update_data_set_input generates UpdateDataSet operation input"""
        update_data_set_input = {
            'AwsAccountId': aws_account_id,
            'DataSetId': self.data_set_id(),
            'ImportMode': self.get('ImportMode'),
            'Name': self.get('Name'),
            'PhysicalTableMap': self.physical_table_map(),
            'LogicalTableMap': self.logical_table_map(),
        }
        column_groups = self.get('ColumnGroups')
        if column_groups is not None:
            update_data_set_input['ColumnGroups'] = column_groups
        column_level_permission_rules = self.get('ColumnLevelPermissionRules')
        if column_level_permission_rules is not None:
            update_data_set_input['ColumnLevelPermissionRules'] = column_level_permission_rules
        data_set_usage_configuration = self.get('DataSetUsageConfiguration')
        if data_set_usage_configuration is not None:
            update_data_set_input['DataSetUsageConfiguration'] = data_set_usage_configuration
        field_folders = self.filed_folders()
        if field_folders is not None:
            update_data_set_input['FieldFolders'] = field_folders
        row_level_permission_data_set = self.get('RowLevelPermissionDataSet')
        if row_level_permission_data_set is not None:
            update_data_set_input['RowLevelPermissionDataSet'] = row_level_permission_data_set
        row_level_permission_tag_configuration = self.get(
            'RowLevelPermissionTagConfiguration')
        if row_level_permission_tag_configuration is not None:
            update_data_set_input['RowLevelPermissionTagConfiguration'] = row_level_permission_tag_configuration  # pylint: disable=line-too-long
        return update_data_set_input

    def to_json(self) -> str:
        """to_json returns JSON string of DataSet"""
        return json.dumps(self.data_set, indent=2, default=str)

    def get(self, key: str) -> Any:
        """get returns value of key"""
        return self.data_set.get(key)

def _insert_meta(target: yaml.CommentedMap) -> None:
    name_pos: Optional[int] = None
    description_pos: Optional[int] = None
    last_pos: int = 0
    for j, key in enumerate(target):
        if key == 'name':
            name_pos = j
        if key == 'description':
            description_pos = j
        last_pos = j
    insert_pos = last_pos
    if description_pos is not None:
        insert_pos = description_pos + 1
    elif name_pos is not None:
        insert_pos = name_pos + 1
    else:
        insert_pos = last_pos + 1
    target.insert(insert_pos, 'meta', {})

class App:
    """App represents dbt_quicksight_lineage application"""

    def __init__(
        self,
        manifest: Manifest,
        quicksight_client: Any = None,
        aws_account_id: Optional[str] = None,
    ) -> None:
        self.manifest = manifest
        if quicksight_client is None:
            self.quicksight_client = boto3.client('quicksight')
        else:
            self.quicksight_client = quicksight_client
        if aws_account_id is None:
            self.aws_account_id = boto3.client(
                'sts').get_caller_identity().get('Account')
        else:
            self.aws_account_id = aws_account_id

    def init(
            self,
            data_set_id: str,
            data_source_arn: Optional[str] = None,
            project_dir: Optional[str] = None,
    ) -> None:
        """
            execute init operation
            download info from data set and write modify schema.yaml
        """
        output = self.quicksight_client.describe_data_set(
            AwsAccountId=self.aws_account_id,
            DataSetId=data_set_id,
        )
        if output.get('Status') != 200:
            raise ValueError(
                f'describe data set failed status: {output.get("Status")}')
        data_set = DataSet(output.get('DataSet'))
        logger.debug(data_set.to_json())
        logger.info("DataSet Name: %s", data_set.get('Name'))
        field_folders = data_set.filed_folders()
        for physical_table_id, node in self._detect_related_nodes(data_set, data_source_arn):
            self._update_schema_yaml(
                data_set,
                physical_table_id,
                node,
                project_dir,
            )

    def update_data_set(
            self,
            data_set_id: str,
            dry_run: bool = False,
    ) -> Tuple[Optional[Any], Dict[str, Any]]:
        """execute update data set operation"""
        output = self.quicksight_client.describe_data_set(
            AwsAccountId=self.aws_account_id,
            DataSetId=data_set_id,
        )
        if output.get('Status') != 200:
            raise ValueError(
                f'describe data set failed status: {output.get("Status")}')
        data_set = DataSet(output.get('DataSet'))
        logger.debug(data_set.to_json())
        logger.info("DataSet Name: %s", data_set.get('Name'))

        for physical_table_id, node in self._detect_modify_target(data_set):
            logger.debug(
                "detect PhysicalTableId: %s, Node: %s",
                physical_table_id,
                node.unique_id,
            )
            physical_relational_table = data_set.physical_relational_table(
                physical_table_id)
            logical_table, field_folders = self._generate_logical_table(
                physical_table_id,
                physical_relational_table,
                node,
            )
            data_set.replace_logical_table(physical_table_id, logical_table)
            physical_columns = physical_relational_table.get(
                'InputColumns', [])
            data_set.replace_field_folder(physical_columns, field_folders)

        update_data_set_input = data_set.generate_update_data_set_input(
            self.aws_account_id)
        logger.debug(json.dumps(update_data_set_input, indent=2))
        if dry_run:
            return None, update_data_set_input

        output = self.quicksight_client.update_data_set(
            **update_data_set_input)
        if output.get('Status') != 200:
            raise ValueError(
                f'update data set failed status: {output.get("Status")}')
        logger.info("Update DataSet: %s", data_set_id)
        logger.debug(json.dumps(output, indent=2, default=str))
        return output, update_data_set_input

    def _find_models(
            self,
    ) -> Iterator[ManifestNode]:
        for node in self.manifest.nodes.values():
            if node.resource_type != 'model':
                continue
            if node.language != 'sql':
                continue
            yield node

    def _find_models_by_data_set(
            self,
            data_set_id: str,
            data_source_arn: Optional[str] = None
    ) -> Iterator[ManifestNode]:
        for node in self._find_models():
            data_sets = node.meta.get('quicksight', {}).get('data_sets', [])
            for target in data_sets:
                logger.debug(
                    "model: %s data_set: %s",
                    node.unique_id,
                    target.get('id'),
                )
                if target.get('id') == data_set_id:
                    if target.get('data_source') is not None:
                        if target.get('data_source') != data_source_arn:
                            continue
                    yield node
                    break

    def _detect_modify_target(
        self,
        data_set: DataSet,
    ) -> Iterator[Tuple[str, ManifestNode]]:
        data_set_id = data_set.data_set_id()
        for physical_table_id, physical_table in data_set.physical_table_map().items():
            relational_table = physical_table.get('RelationalTable')
            if relational_table is None:
                continue
            data_source_arn = relational_table.get('DataSourceArn')
            schema = relational_table.get('Schema')
            identifier = relational_table.get('Name')
            logger.debug(
                "check PhysicalTableId: %s (data_set_id=%s data_source=%s)",
                physical_table_id,
                data_set_id,
                data_source_arn,
            )
            if data_source_arn is None or schema is None or identifier is None:
                continue
            for node in self._find_models_by_data_set(data_set_id, data_source_arn):
                logger.debug(
                    "match check node=%s (table=%s.%s)",
                    node.unique_id,
                    schema,
                    identifier,
                )
                if node.schema == schema and node.alias == identifier:
                    yield physical_table_id, node

    def _generate_logical_table_alias(self, node: ManifestNode) -> str:
        return node.meta.get('quicksight', {}).get('logical_table_name', []) or node.alias

    def _generate_logical_table(
            self,
            physical_table_id: str,
            physical_relational_table: Dict[str, Any],
            node: ManifestNode,
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        logical_table = {
            'Alias': self._generate_logical_table_alias(node),
            'Source': {
                'PhysicalTableId': physical_table_id,
            }
        }
        data_transforms = []
        folders = {
            folder_info['name'].rstrip('/'): {
                'description': folder_info.get('description', ''),
            } for folder_info in node.meta.get('quicksight', {}).get('folders', [])
        }
        projectioned_columns = []
        physical_columns = physical_relational_table.get('InputColumns', [])
        for column_name, column_info in node.columns.items():
            if column_name in physical_columns:
                logger.debug(
                    "column %s already exists in physical table",
                    column_name,
                )
                continue
            quicksight_meta = column_info.meta.get('quicksight', {})
            field_name = quicksight_meta.get('field_name')
            if field_name is None:
                field_name = column_name
            else:
                logger.info(
                    "column %s will be renamed to %s",
                    column_name,
                    field_name,
                )
                data_transforms.append({
                    'RenameColumnOperation': {
                        'ColumnName': column_name,
                        'NewColumnName': field_name,
                    }
                })

            if column_info.description is not None:
                data_transforms.append({
                    'TagColumnOperation': {
                        'ColumnName': field_name,
                        'Tags': [
                            {
                                'ColumnDescription': {
                                    'Text': column_info.description,
                                }
                            }
                        ]
                    }
                })
            folder_name = quicksight_meta.get('folder')
            if folder_name is not None:
                folder_name = folder_name.rstrip('/')
                folder = folders.get(folder_name)
                if folder is None:
                    folder = {}
                    folders[folder_name] = folder
                folder['columns'] = folder.get('columns', []) + [field_name]
            projectioned_columns.append(field_name)

        if len(projectioned_columns) > 0:
            data_transforms.append({
                'ProjectOperation': {
                    'ProjectedColumns': projectioned_columns,
                }
            })
        logical_table['DataTransforms'] = data_transforms
        return logical_table, folders

    def _detect_related_nodes(
        self,
        data_set: DataSet,
        data_source_arn: Optional[str] = None,
    ) -> Iterator[Tuple[str, ManifestNode]]:
        """
            detect related nodes from manifest

            related condition:
            if data_source_arn is not none, same data_set_arn
            node.resource_type == 'model'
            node.language == 'sql'
            node.schema == physical_table.schema
            node.alias == physical_table.name

            return tuple of (physical_tabel_id, model_node)
        """
        data_set_id = data_set.data_set_id()
        for physical_table_id, physical_table in data_set.physical_table_map().items():
            relational_table = physical_table.get('RelationalTable')
            if relational_table is None:
                continue
            if data_source_arn is not None:
                if relational_table.get('DataSourceArn') != data_source_arn:
                    continue
            schema = relational_table.get('Schema')
            identifier = relational_table.get('Name')
            logger.debug(
                "check PhysicalTableId: %s (data_set_id=%s data_source=%s)",
                physical_table_id,
                data_set_id,
                data_source_arn,
            )
            if schema is None or identifier is None:
                continue
            for node in self._find_models():
                logger.debug(
                    "match check node=%s (table=%s.%s)",
                    node.unique_id,
                    schema,
                    identifier,
                )
                if node.schema == schema and node.alias == identifier:
                    yield physical_table_id, node

    def _generate_schema_dict(  # pylint: disable=too-many-locals
            self,
            model_name: str,
            data_set: DataSet,
            physical_table_id: str,
    ) -> Dict[str, Any]:
        physical_table = data_set.physical_relational_table(physical_table_id)
        _, logical_table = data_set.find_logical_table_by_physical_table_id(
            physical_table_id,
        )
        model = {
            'name':  model_name,
            'meta': {
                'quicksight': {
                    'logical_table_name': logical_table['Alias'] or model_name,
                    'data_sets': [
                        {
                            'id': data_set.data_set_id(),
                            'data_source_arn': physical_table['DataSourceArn'],
                        },
                    ]
                },
            },
            'columns': [],
        }
        columns = {}
        for operation in logical_table.get('DataTransforms', []):
            rename_operation = operation.get('RenameColumnOperation')
            if rename_operation is not None:
                logical_column_name = rename_operation['NewColumnName']
                physical_column_name = rename_operation['ColumnName']
                columns[logical_column_name] = {
                    'name': physical_column_name,
                    'meta': {
                        'quicksight': {
                            'field_name': logical_column_name,
                        },
                    },
                }
                continue
            tag_operation = operation.get('TagColumnOperation')
            if tag_operation is not None:
                logical_column_name = tag_operation['ColumnName']
                column = columns.get(logical_column_name, {})
                for tag in tag_operation['Tags']:
                    if 'ColumnDescription' in tag:
                        column['description'] = tag['ColumnDescription']['Text']
                continue
        for field_folder_path, field_folder in (data_set.filed_folders() or {}).items():
            if field_folder.get('description') is not None:
                meta_folders = model['meta']['quicksight'].get('folders', [])
                meta_folders.append({
                    'name': field_folder_path,
                    'description': field_folder['description'],
                })
                model['meta']['quicksight']['folders'] = meta_folders
            for column_name in field_folder['columns']:
                column = columns.get(column_name, {})
                column['meta'] = column.get('meta', {})
                column['meta']['quicksight'] = column['meta'].get(
                    'quicksight', {})
                column['meta']['quicksight']['folder'] = field_folder_path
                columns[column_name] = column
        model['columns'] = list(columns.values())
        schema = {
            'models': [model],
        }
        return schema



    def _update_schema_yaml(  # pylint: disable=too-many-locals
            self,
            data_set: DataSet,
            physical_table_id: str,
            node: ManifestNode,
            project_dir: Optional[str] = None,
    ) -> None:
        package_name, existing_file_path = node.patch_path.split("://")
        if project_dir is not None:
            existing_file_path = os.path.join(project_dir, existing_file_path)
        logger.debug(
            "target project: %s schema file path: %s",
            package_name,
            existing_file_path,
        )
        yaml_handler = yaml.YAML()
        yaml_handler.indent(mapping=2, sequence=4, offset=2)
        yaml_handler.width = 800
        yaml_handler.preserve_quotes = True
        yaml_handler.default_flow_style = False

        with open(existing_file_path, 'r', encoding='utf-8') as f:
            schema = yaml_handler.load(f)
        generated_schema = self._generate_schema_dict(
            node.name,
            data_set,
            physical_table_id,
        )

        # yaml marge: same name in list, overwrite
        for schema_model in generated_schema['models']:
            for i, model in enumerate(schema['models']):
                if model['name'] != schema_model['name']:
                    continue
                meta = schema['models'][i].get('meta')
                if meta is None:
                    _insert_meta(schema['models'][i])
                    meta = {}
                meta.update(schema_model.get('meta', {}))
                schema['models'][i]['meta'] = meta
                for column in schema_model.get('columns', []):
                    for j, target_column in enumerate(schema['models'][i]['columns']):
                        if target_column['name'] != column['name']:
                            continue
                        meta = schema['models'][i]['columns'][j].get('meta')
                        if meta is None:
                            _insert_meta(schema['models'][i]['columns'][j])
                            meta = {}
                        meta.update(column.get('meta', {}))
                        schema['models'][i]['columns'][j]['meta'] = meta
                        if 'description' in column and 'description' not in target_column:
                            schema['models'][i]['columns'][j]['description'] = column['description']
        with open(existing_file_path, 'w', encoding='utf-8') as stream:
            yaml_str = yaml_handler.dump(schema, stream)
            logger.debug("updated schema yaml: %s", yaml_str)
            logger.info("updated schema yaml: %s", existing_file_path)
