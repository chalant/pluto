'''
1) create a hub client (url is provided in cli)
    1.1) download and cache resources (calendars, exchanges...)
2) create and launch dev server
2) create and launch test controller
'''


from os import path

import click

from dev import strategy_store

from pluto import pluto_setup
from pluto import examples
from pluto.data.bundles import test


@click.group()
def main():
    pass


@main.command()
def install_examples():
    print("Installing examples...")

    root_dir = pluto_setup.create_directory(path.expanduser('~/.pluto'))

    store = strategy_store.StrategyStore(root_dir)

    # copy examples into directory
    for name, ap in examples.EXAMPLE_MODULES.items():
        args, path_ = ap

        try:
            store.add_strategy(name, path_)
        except FileExistsError:
            pass

    test.TestBundle(root_dir).ingest()

    print("Done")


@main.command()
@click.argument('name')
def add_manager(name):
    root_dir = pluto_setup.create_directory(path.expanduser('~/.pluto'))

    launchers = root_dir.get_directory('launchers')

    if launchers.contains_file(name, 'py'):
        print("File Already Exists...")
        return

    launchers.add_file(name, 'py').touch()

    print('File created')


@main.command()
@click.argument('name')
@click.option('-t', '--template', default='pipeline')
def add_strategy(name, template):
    root_dir = pluto_setup.create_directory(path.expanduser('~/.pluto'))
    store = strategy_store.StrategyStore(root_dir)

    templates_dir = root_dir.get_directory('strategies').get_directory('templates')

    if not templates_dir.contains_file(template, 'py'):
        print("Template of name {} does not exist".format(template))
        return

    template_file = templates_dir.get_file(template, 'py')

    if name in store:
        print("Strategy named {} already exits".format(name))
        return

    store.add_strategy(
        name,
        template_file.path
    )

    print("Strategy added")


if __name__ == "__main__":
    # todo: directories should be created during installation, not when we launch
    #  the application...
    main()

