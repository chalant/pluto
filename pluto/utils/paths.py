from zipline.utils import paths
import os

def get_contrib_root(environ=None):
    zp_root = paths.zipline_root(environ)
    root = os.path.join(zp_root,'contrib')
    paths.ensure_directory(root)
    return root

def create_file(path,environ=None):
    assert os.path.isfile(path)
    if path.startswith('/'):
        path = path.replace('/','',1)
    paths.ensure_file(path)

def get_file_path(path,environ=None):
    '''returns a list of strings'''
    path = os.path.join(get_contrib_root(environ), path)
    if not os.path.exists(path):
        create_file(path, environ)
    return path

def create_directory(name,environ=None):
    pt = os.path.join(get_contrib_root(environ),name)
    if not os.path.exists(pt):
        os.mkdir(pt)
    return pt

