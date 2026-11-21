'''
1) create a hub client (url is provided in cli)
    1.1) download and cache resources (calendars, exchanges...)
2) create and launch dev server
2) create and launch test controller
'''


from os import path

import click

from pluto import examples
from pluto.data.bundles import test
from pluto.setup import directory_setup


@click.group()
def main():
    pass


@main.command()
def install_examples():
    print("Installing examples...")

    root_dir = directory_setup.create_directory(path.expanduser('~/.pluto'))

    # copy the examples to <root>/strategies/<name>.py (existing files are kept)
    for name, ap in examples.EXAMPLE_MODULES.items():
        args, path_ = ap

        try:
            directory_setup.add_strategy_script(root_dir, name, path_)
        except FileExistsError:
            pass

    test.TestBundle(root_dir).ingest()

    print("Done")


if __name__ == "__main__":
    # todo: directories should be created during installation, not when we launch
    #  the application...
    main()

