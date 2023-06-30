import json
import os
from typing import Optional
from dbt.contracts.graph.manifest import Manifest

# The ManifestLoader is responsible for loading DBT Manifests from the DBT


class ManifestLoader:
    def __init__(
        self,
        manifest_path: Optional[str] = None,
    ) -> None:
        self.manifest_path = manifest_path

    def load_manifest(self) -> Manifest:
        if self.manifest_path is not None:
            extension = self._get_extension()
            if extension == '.json':
                return self._load_from_json()
            if extension == '.msgpack':
                return self._load_from_msgpack()

        raise ValueError(f'Invalid loader parameter')

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
