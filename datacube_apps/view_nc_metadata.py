#!/bin/env python3
"""
Display embedded metadata from a Data Cube style NetCDF file.


"""
import json
from io import StringIO

import click
import xarray as xr
import yaml


@click.command()
@click.argument('filename')
@click.option('--yaml', 'output_format', flag_value='yaml', default=True)
@click.option('--json', 'output_format', flag_value='json')
@click.option('--time', type=int)
def main(filename, output_format, variable_name='dataset', time=0):
    ds = xr.open_dataset(filename)

    document = bytes(ds.data_vars[variable_name].data).decode('utf8')

    if output_format == 'json':
        obj = yaml.load(StringIO(document))
        # Disable pylint check for the following line for Python 2.7
        document = json.dumps(obj, indent=4)  # pylint: disable=redefined-variable-type

    output = format_document(document, output_format)
    click.echo_via_pager(output)


def format_document(document, language):
    try:
        from pygments.lexers import get_lexer_by_name
        from pygments.formatters.terminal import TerminalFormatter
        from pygments import highlight

        lexer = get_lexer_by_name(language)
        return highlight(document, lexer, TerminalFormatter(bg='dark'))
    except ImportError:
        return document


if __name__ == '__main__':
    main()
