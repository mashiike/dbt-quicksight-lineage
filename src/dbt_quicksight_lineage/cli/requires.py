"""This module contains decorators for CLI commands that require wrappers"""
from typing import Any, Dict, Optional
from functools import update_wrapper
from pathlib import Path
import click
from ruamel import yaml
from dbt_quicksight_lineage.core import ManifestLoader


def dbt_manifest(func):
    """Decorator for CLI commands that require a dbt manifest"""

    @click.option(
        "--manifest-path",
        "-m",
        type=click.Path(exists=True),
        help="Path to manifest.json or partial_parse.msgpack",
    )
    @click.option(
        "--profiles-dir",
        type=click.Path(exists=True),
        default=Path.home() / ".dbt",
        help="Path to profiles.yml directory",
    )
    @click.option(
        "--project-dir",
        type=click.Path(exists=True),
        help="Path to dbt project directory",
        default=".",
    )
    @click.option(
        "--target",
        type=str,
        help="Name of target profile to use",
    )
    @click.option(
        "--profile",
        type=str,
        help="Name of profile to use",
    )
    @click.option(
        "--vars",
        type=str,
        help="JSON string of variables to pass to dbt",
    )
    def wrapper(*args, **kwargs):
        ctx = args[0]
        assert isinstance(ctx, click.Context)
        ctx.obj = ctx.obj or {}
        cli_vars_str = kwargs.get('vars')
        cli_vars: Optional[Dict[str, Any]] = None
        if cli_vars_str is not None:
            cli_vars = yaml.safe_load(cli_vars_str)

        loader = ManifestLoader(
            manifest_path=kwargs.get('manifest_path'),
            project_dir=kwargs.get('project_dir'),
            profiles_dir=kwargs.get('profiles_dir'),
            profile=kwargs.get('profile'),
            target=kwargs.get('target'),
            cli_vars=cli_vars,
        )
        try:
            ctx.obj['manifest'] = loader.load_manifest()
        except ValueError as ex:
            click.echo(
                f"cannot load manifest, check --manifest-path flag: {ex}",
                err=True,
            )
            raise click.Abort()
        return func(*args, **kwargs)
    return update_wrapper(wrapper, func)
