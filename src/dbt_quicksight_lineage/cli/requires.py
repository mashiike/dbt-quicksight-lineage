import click
from functools import update_wrapper
from dbt_quicksight_lineage.core import ManifestLoader

def dbt_manifest(func):
    def wrapper(*args, **kwargs):
        ctx = args[0]
        assert isinstance(ctx, click.Context)
        ctx.obj = ctx.obj or {}
        loader = ManifestLoader(
            manifest_path=kwargs.get('manifest_path'),
        )
        try:
            ctx.obj['manifest'] = loader.load_manifest()
        except ValueError as e:
            click.echo("cannot load manifest, check --manifest-path flag: {}".format(e), err=True)
            raise click.Abort()
        return func(*args, **kwargs)
    return update_wrapper(wrapper, func)
