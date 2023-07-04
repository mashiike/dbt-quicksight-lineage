import json
import os
from typing import Optional, Dict, Any
from dbt.config.runtime import  RuntimeConfig
from dbt.flags import set_from_args
from dbt.adapters.factory import get_adapter_class_by_name, register_adapter, reset_adapters
from dbt.contracts.graph.manifest import Manifest
from dbt.parser.manifest import ManifestLoader as DbtManifestLoader

class DbtArgs:
    def __init__(
        self,
        profiles_dir: Optional[str] = None,
        profile: Optional[str] = None,
        target: Optional[str] = None,
        project_dir: Optional[str] = None,
        cli_vars: Optional[Dict[str, Any]] = None,
        threads: Optional[int] = None,
    ) -> None:
        self.profiles_dir = profiles_dir
        self.profile = profile
        self.target = target
        self.project_dir = project_dir
        self.cli_vars = cli_vars or {}
        self.threads = threads

    @property
    def vars(self) -> Dict[str, Any]:
        return self.cli_vars or {}

# The ManifestLoader is responsible for loading DBT Manifests from the DBT
class ManifestLoader:
    def __init__(
        self,
        manifest_path: Optional[str] = None,
        project_dir: Optional[str] = None,
        profiles_dir: Optional[str] = None,
        profile: Optional[str] = None,
        target: Optional[str] = None,
        cli_vars: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.manifest_path = manifest_path
        self.project_dir = project_dir
        self.profiles_dir = profiles_dir
        self.target = target
        self.cli_vars = cli_vars
        self.profile = profile

    def load_manifest(self) -> Manifest:
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
        set_from_args(args,args)
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
        with open(self.manifest_path) as f:
            d = json.load(f)
        return Manifest.from_dict(d)

    def _load_from_msgpack(self) -> Manifest:
        with open(self.manifest_path, 'rb') as f:
            d = f.read()
        return Manifest.from_msgpack(d)
