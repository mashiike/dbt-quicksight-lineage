"""dbt_quicksight_lineage.core.dbt: provides dbt-core project parser."""
import json
import os
from typing import Optional, Dict, Any, Iterator
from dataclasses import dataclass
from dbt.config.runtime import RuntimeConfig
from dbt.flags import set_from_args
from dbt.adapters.factory import get_adapter_class_by_name, register_adapter, reset_adapters
from dbt.contracts.graph.manifest import Manifest, ManifestNode
from dbt.parser.manifest import ManifestLoader as DbtManifestLoader


@dataclass
class DbtArgs:
    """dbt-core cli arguments mock"""

    profiles_dir: Optional[str] = None
    profile: Optional[str] = None
    target: Optional[str] = None
    project_dir: Optional[str] = None
    cli_vars: Optional[Dict[str, Any]] = None
    threads: Optional[int] = None

    @property
    def vars(self) -> Dict[str, Any]:
        """dbt-core cli vars mock"""
        return self.cli_vars or {}


@dataclass
class ManifestLoader:
    """the ManifestLoader is responsible for loading DBT Manifests from the DBT"""

    manifest_path: Optional[str] = None
    project_dir: Optional[str] = None
    profiles_dir: Optional[str] = None
    profile: Optional[str] = None
    target: Optional[str] = None
    cli_vars: Optional[Dict[str, Any]] = None

    def load_manifest(self) -> Manifest:
        """load the DBT manifest"""
        if self.manifest_path is not None:
            extension = self._get_extension()
            if extension == '.json':
                return self._load_from_json()
            if extension == '.msgpack':
                return self._load_from_msgpack()
            raise ValueError(f'unknown extension: {extension}')

        args = DbtArgs(
            profiles_dir=self.profiles_dir,
            profile=self.profile,
            target=self.target,
            project_dir=self.project_dir,
            cli_vars=self.cli_vars,
        )
        set_from_args(args, args)
        config = RuntimeConfig.from_args(args)
        adapter = get_adapter_class_by_name(config.credentials.type)(config)
        config.adapter = adapter
        register_adapter(config)
        parser = DbtManifestLoader(
            config,
            config.load_dependencies(),
            adapter.connections.set_query_header,
        )
        manifest = parser.load()
        reset_adapters()
        return manifest

    def _get_extension(self):
        _, ext = os.path.splitext(self.manifest_path)
        return ext

    def _load_from_json(self) -> Manifest:
        with open(self.manifest_path, encoding="utf-8") as f:
            data = json.load(f)
        return Manifest.from_dict(data)

    def _load_from_msgpack(self) -> Manifest:
        with open(self.manifest_path, 'rb') as f:
            data = f.read()
        return Manifest.from_msgpack(data)

class ManifestNodeExplorer:
    """
    The ManifestNodeExplorer is responsible for exploring the DBT Manifest node
    """
    def __init__(
        self,
        node: ManifestNode,
    ) -> None:
        self._node = node

    @property
    def node_alias(self) -> str:
        """return the node alias from the node"""
        return self._node.alias

    @property
    def quicksight_meta(self) -> Dict[str, Any]:
        """return the quicksight meta data from the node"""
        return self._node.meta.get('quicksight', {})

    @property
    def logical_table_name(self) -> Optional[str]:
        """return the logical table name from the node"""
        return self.quicksight_meta.get('logical_table_name') or self.node_alias

    @property
    def field_folders(self) -> Iterator[Dict[str, Any]]:
        """return the field folders from the node"""
        return iter(self.quicksight_meta.get('folders', []))

    @property
    def column_names(self) -> Iterator[str]:
        """return the column names from the node"""
        return iter(self._node.columns.keys())

    def get_columnn_quicksight_meta(
        self,
        column_name: str,
    ) -> Dict[str, Any]:
        """return the column quicksight meta data from the node"""
        column = self._node.columns.get(column_name)
        if column is None:
            return {}
        return column.meta.get('quicksight', {})

    def get_field_name(
        self,
        column_name: str,
    ) -> Optional[str]:
        """return the field name from the node"""
        return self.get_columnn_quicksight_meta(column_name).get('field_name')

    def get_description(
        self,
        column_name: str,
    ) -> Optional[str]:
        """return the description from the node"""
        column = self._node.columns.get(column_name)
        if column is None:
            return None
        return column.description

    def is_hidden(
        self,
        column_name: str,
    ) -> Optional[bool]:
        """return the hidden from the node"""
        return self.get_columnn_quicksight_meta(column_name).get('hidden', False)


    def get_folder(
        self,
        column_name: str,
    ) -> Optional[str]:
        """return the folder from the node"""
        return self.get_columnn_quicksight_meta(column_name).get('folder')

    def get_geographic_role(
        self,
        column_name: str,
    ) -> Optional[str]:
        """return the geographic role from the node"""
        return self.get_columnn_quicksight_meta(column_name).get('geographic_role')

    def get_data_type(
        self,
        column_name: str,
    ) -> Optional[str]:
        """return the data type from the node"""
        return self.get_columnn_quicksight_meta(column_name).get('data_type')
