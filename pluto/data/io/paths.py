from pluto.interface.utils import paths

ROOT = paths.get_dir('data')


def bundle(bundle_name):
    return paths.get_dir(bundle_name, ROOT)

def daily_bars(bundle_name):
    return paths.get_file_path('daily_equity', bundle(bundle_name))

def minute_bars(bundle_name):
    return paths.get_file_path('minute_equity', bundle(bundle_name))

def assets(bundle_name):
    return paths.get_file_path('assets', bundle(bundle_name))

def adjustments(bundle_name):
    return paths.get_file_path('adjustments', bundle(bundle_name))