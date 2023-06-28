# SPDX-FileCopyrightText: 2023-present mashiike <ikeda-masashi@kayac.com>
#
# SPDX-License-Identifier: MIT
import click

from dbt_quicksight_lineage.__about__ import __version__


@click.group(context_settings={"help_option_names": ["-h", "--help"]}, invoke_without_command=True)
@click.version_option(version=__version__, prog_name="dbt_quicksight_lineage")
def dbt_quicksight_lineage():
    click.echo("Hello world!")
