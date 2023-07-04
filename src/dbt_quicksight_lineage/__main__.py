"""This is main module of dbt-quicksight-lineage. only entrypoint"""
# SPDX-FileCopyrightText: 2023-present mashiike <ikeda-masashi@kayac.com>
#
# SPDX-License-Identifier: MIT
import sys

if __name__ == "__main__":
    from dbt_quicksight_lineage.cli import dbt_quicksight_lineage

    sys.exit(dbt_quicksight_lineage()) #ignore
