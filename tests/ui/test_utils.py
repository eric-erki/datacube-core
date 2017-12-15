from pathlib import Path

import click
from click.testing import CliRunner

from datacube.ui.click import PathlibPath


def test_click_pathlib_path():
    runner = CliRunner()

    @click.command()
    @click.argument('path', type=PathlibPath(exists=True, file_okay=True, dir_okay=False))
    def testpath(path):
        assert isinstance(path, Path)

    with runner.isolated_filesystem():
        result = runner.invoke(testpath, ['hello.txt'])
        assert result.exit_code != 0

        Path('hello.txt').touch()

        result = runner.invoke(testpath, ['hello.txt'])
        assert result.exit_code == 0
