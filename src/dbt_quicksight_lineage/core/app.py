"""dbt_quicksight_lineage.core.app is application core logic"""
import logging
import json
import os
from typing import Iterator, Optional, Any, Dict, Tuple
import boto3
from ruamel import yaml
from dbt.contracts.graph.manifest import Manifest, ManifestNode
from dbt_quicksight_lineage.core.quicksight import DataSet, PhysicalTable
from dbt_quicksight_lineage.core.dbt import ManifestNodeExplorer
logger = logging.getLogger()


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
        for physical_table, node in self._detect_related_nodes(data_set, data_source_arn):
            self._update_schema_yaml(
                data_set,
                physical_table.physical_table_id,
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

        for physical_table, node in self._detect_modify_target(data_set):
            logger.debug(
                "detect PhysicalTableId: %s, Node: %s",
                physical_table.physical_table_id,
                node.unique_id,
            )
            self._modify_logical_table(
                data_set,
                physical_table,
                node,
            )
        update_data_set_input = data_set.generate_update_data_set_input(
            self.aws_account_id
        )
        logger.debug(json.dumps(update_data_set_input, indent=2, default=str))
        if dry_run:
            return None, update_data_set_input
        output = self.quicksight_client.update_data_set(
            **update_data_set_input
        )
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
    ) -> Iterator[Tuple[PhysicalTable, ManifestNode]]:
        data_set_id = data_set.data_set_id
        for physical_table in data_set.find_relational_table():
            data_source_arn = physical_table.data_source_arn
            schema = physical_table.schema_name
            identifier = physical_table.table_name
            logger.debug(
                "check PhysicalTableId: %s (data_set_id=%s data_source=%s)",
                physical_table.physical_table_id,
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
                    yield physical_table, node

    def _modify_logical_table(
        self,
        data_set: DataSet,
        physical_table: PhysicalTable,
        node: ManifestNode,
    ) -> None:
        explorer = ManifestNodeExplorer(node)
        physical_table_id = physical_table.physical_table_id
        alias = explorer.logical_table_name
        if alias is not None:
            data_set.set_alias(
                physical_table_id,
                alias,
            )
        for field_folder in explorer.field_folders:
            data_set.add_field_folder(
                field_folder['name'],
                field_folder.get('description')
            )
        for column_name in explorer.column_names:
            field_name = explorer.get_field_name(column_name)
            if field_name is not None:
                data_set.set_rename_column_operation(
                    physical_table_id,
                    column_name,
                    field_name,
                )
            description = explorer.get_description(column_name)
            if description is not None:
                data_set.set_tag_column_description_operation(
                    physical_table_id,
                    column_name,
                    description,
                )
            geograpic_role = explorer.get_geographic_role(column_name)
            if geograpic_role is not None:
                data_set.set_tag_column_geographic_role_operation(
                    physical_table_id,
                    column_name,
                    geograpic_role,
                )
            data_type = explorer.get_data_type(column_name)
            if data_type is not None:
                data_set.set_cast_column_type_operation(
                    physical_table_id,
                    column_name,
                    data_type,
                )
            if explorer.is_hidden(column_name):
                data_set.remove_from_projected_columns(
                    physical_table_id,
                    column_name,
                )
            else:
                data_set.add_to_projected_columns(
                    physical_table_id,
                    column_name,
                )
            folder = explorer.get_folder(column_name)
            if folder is not None:
                data_set.add_to_field_folder(
                    physical_table_id,
                    column_name,
                    folder,
                )

    def _detect_related_nodes(
        self,
        data_set: DataSet,
        data_source_arn: Optional[str] = None,
    ) -> Iterator[Tuple[PhysicalTable, ManifestNode]]:
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
        data_set_id = data_set.data_set_id
        for physical_table in data_set.find_relational_table():
            if data_source_arn is not None:
                if physical_table.data_source_arn != data_source_arn:
                    continue
            logger.debug(
                "check PhysicalTableId: %s (data_set_id=%s data_source=%s)",
                physical_table.physical_table_id,
                data_set_id,
                physical_table.data_source_arn,
            )
            schema = physical_table.schema_name
            identifier = physical_table.table_name
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
                    yield physical_table, node

    def _generate_schema_dict(  # pylint: disable=too-many-locals
            self,
            model_name: str,
            data_set: DataSet,
            physical_table_id: str,
    ) -> Dict[str, Any]:
        # only 1st logical table
        physical_table = data_set.physical_table_map[physical_table_id]
        for logical_table in data_set.find_logical_by_physical(physical_table_id):
            model = {
                'name':  model_name,
                'meta': {
                    'quicksight': {
                        'logical_table_name': logical_table.get('Alias') or model_name,
                        'data_sets': [
                            {
                                'id': data_set.data_set_id,
                                'data_source_arn': physical_table.data_source_arn,
                            },
                        ]
                    },
                },
                'columns': [],
            }
            for physical_column in physical_table.columns:
                column = {
                    'name': physical_column['Name'],
                }
                is_append = False
                logical_column_name = logical_table.get_logical_column_name(
                    physical_column['Name'],
                )
                if logical_column_name is not None:
                    column['meta'] = {
                        'quicksight': {
                            'field_name': logical_column_name,
                        },
                    }
                    is_append = True
                if not logical_table.contains_projected_columns(
                    physical_column['Name'],
                ):
                    column['meta'] = column.get('meta', {})
                    column['meta']['quicksight'] = column['meta'].get(
                        'quicksight', {})
                    column['meta']['quicksight']['hidden'] = True
                    is_append = True

                description = logical_table.get_tag_column_description(
                    physical_column['Name'],
                )
                if description is not None:
                    column['description'] = description
                    is_append = True
                geograpic_role = logical_table.get_tag_column_geographic_role(
                    physical_column['Name'],
                )
                if geograpic_role is not None:
                    column['meta'] = column.get('meta', {})
                    column['meta']['quicksight'] = column['meta'].get(
                        'quicksight', {})
                    column['meta']['quicksight']['geographic_role'] = geograpic_role.lower()
                    is_append = True

                field_folder_path = data_set.get_field_folder_path(
                    physical_table.physical_table_id,
                    physical_column['Name'],
                )
                cast_column_type = logical_table.get_cast_column_type(
                    physical_column['Name'],
                )
                if cast_column_type is not None:
                    column['meta'] = column.get('meta', {})
                    column['meta']['quicksight'] = column['meta'].get(
                        'quicksight', {})
                    column['meta']['quicksight']['data_type'] = cast_column_type.lower()
                    is_append = True
                if field_folder_path is not None:
                    column['meta'] = column.get('meta', {})
                    column['meta']['quicksight'] = column['meta'].get(
                        'quicksight', {})
                    column['meta']['quicksight']['folder'] = field_folder_path
                    is_append = True

                if is_append:
                    model['columns'].append(column)

            for field_folder in data_set.field_folders.values():
                if field_folder.description is not None:
                    meta_folders = model['meta']['quicksight'].get(
                        'folders', [])
                    meta_folders.append({
                        'name': field_folder.field_folder_path,
                        'description': field_folder.description,
                    })
                    model['meta']['quicksight']['folders'] = meta_folders
            return {'models': [model]}
        return {'models': []}

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
