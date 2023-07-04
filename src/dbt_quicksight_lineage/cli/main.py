"""dbt-quicksight-lineage: DBT to QuickSight Lineage commandline definition"""
import sys
import logging
from typing import Optional
import colorlog
import click
from dbt_quicksight_lineage.cli import requires
from dbt_quicksight_lineage.core import App
from dbt_quicksight_lineage.__about__ import __version__


@click.group(context_settings={"help_option_names": ["-h", "--help"]}, invoke_without_command=True)
@click.version_option(version=__version__, prog_name="dbt_quicksight_lineage")
@click.pass_context
@click.option(
    "--log-level",
    help="Set log level",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
    default="INFO",
)
@click.option(
    "--no-color/--color",
    help="Disable color in output",
    default=None,
)
def dbt_quicksight_lineage(
    ctx: click.Context,
    log_level: str,
    no_color: Optional[bool] = None,
):
    """dbt-quicksight-lineage: DBT to QuickSight Lineage command helper"""
    set_color = False
    if no_color is not None:
        set_color = not no_color
    elif sys.stdout.isatty():
        set_color = True
    if set_color:
        formatter = colorlog.ColoredFormatter(
            '%(log_color)s%(levelname)s:%(name)s:%(message)s',
            log_colors={
                'DEBUG': 'blue',
                'INFO': 'green',
                'WARNING': 'yellow',
                'ERROR': 'red',
                'CRITICAL': 'bold_red',
            }
        )
    else:
        formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger = logging.getLogger()
    logger.addHandler(stream_handler)
    logging.basicConfig(level=log_level)
    logger.setLevel(log_level)
    if log_level == "DEBUG":
        logger.debug("Debug logging enabled")
    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())
        return


@dbt_quicksight_lineage.command()
@click.pass_context
@click.option(
    "--data-set-id",
    type=str,
    help="QuickSight DataSet ID",
    required=True,
)
@click.option(
    "--data-source-arn",
    type=str,
    help="QuickSight DataSource ARN",
)
@requires.dbt_manifest
def init(
    ctx: click.Context,
    data_set_id: str,
    data_source_arn: Optional[str] = None,
    project_dir: Optional[str] = None,
    **_kwargs,
):
    """Modify schema.yml to add QuickSight metadata with Data Set"""
    app = App(
        manifest=ctx.obj['manifest'],
    )
    click.echo(
        f"Describe DataSet: {data_set_id} on {app.aws_account_id}")
    app.init(
        data_set_id=data_set_id,
        data_source_arn=data_source_arn,
        project_dir=project_dir,
    )


@dbt_quicksight_lineage.command()
@click.pass_context
@click.option(
    "--data-set-id",
    type=str,
    help="QuickSight DataSet ID",
    required=True,
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Dry run",
)
@requires.dbt_manifest
def update_data_set(
    ctx: click.Context,
    data_set_id: str,
    dry_run: bool,
    **_kwargs,
):
    """Update QuickSight DataSet from DBT Manifest"""
    app = App(
        manifest=ctx.obj['manifest'],
    )
    click.echo(
        f"Updating QuickSight DataSet: {data_set_id} on {app.aws_account_id}")
    app.update_data_set(
        data_set_id=data_set_id,
        dry_run=dry_run,
    )
