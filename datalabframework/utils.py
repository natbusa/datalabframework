import errno
import os

import json
import jsonschema

import sys
from ruamel.yaml import YAML
from ruamel.yaml.compat import StringIO

class StringDumpYAML(YAML):
    def dump(self, data, stream=None, **kw):
        inefficient = False
        if stream is None:
            inefficient = True
            stream = StringIO()
        YAML.dump(self, data, stream, **kw)
        if inefficient:
            return stream.getvalue()

yaml=StringDumpYAML()
yaml.preserve_quotes = True
yaml.indent(mapping=4, sequence=4, offset=2)

from jinja2 import Environment, Template, filters

from copy import deepcopy

import git

def merge(a, b):
    if not a:
        a = dict()

    if not b:
        b = dict()

    if isinstance(b, dict) and isinstance(a, dict):
        a_and_b = set(a.keys()) & set(b.keys())
        every_key = set(a.keys()) | set(b.keys())
        return {k: merge(a[k], b[k]) if k in a_and_b else
                   deepcopy(a[k] if k in a else b[k]) for k in every_key}

    return deepcopy(b)

def os_sep(path):
    return path.replace("/", os.sep)

def lrchop(s, b='', e=''):
    if s.startswith(b) and len(b)>0:
        s = s[len(b):]
    if s.endswith(e) and len(e)>0:
        s = s[:-len(e)]
    return s

def relative_filename(fullpath_filename, rootpath=os.sep):
    r = lrchop(fullpath_filename,rootpath)
    return r.lstrip(os.sep) if r and r[0]==os.sep else r

def absolute_filename(s, rootpath='.'):
    return s if s.startswith(os.sep) else '{}{}{}'.format(rootpath,os.sep,s)

def breadcrumb_path(fullpath, rootpath=os.sep):
    return '.' + relative_filename(fullpath, rootpath).replace(os.sep,'.')

def get_project_files(ext, rootpath='.', exclude_dirs=[], ignore_dir_with_file='', relative_path=True):
    top  = rootpath

    lst = list()
    for root, dirs, files in os.walk(top, topdown=True):
        for d in exclude_dirs:
            if d in dirs:
                dirs.remove(d)

        if ignore_dir_with_file in files:
            dirs[:] = []
            next
        else:
            for file in files:
                if file.endswith(ext):
                    f = os.path.join(root, file)
                    lst.append(relative_filename(f,rootpath) if relative_path else f)

    return lst

#get_project_files(ext='metadata.yml', ignore_dir_with_file='metadata.ignore.yml', relative_path=False)
#get_project_files(ext='.ipynb', exclude_dirs=['.ipynb_checkpoints'])

def pretty_print(metadata):
    yaml.dump(metadata, sys.stdout)

def render(m, passes=3):
    env = Environment()
    env.globals['env'] = lambda key, value=None: os.getenv(key, value)
    env.filters['env'] = lambda value, key: os.getenv(key, value)

    doc = yaml.dump(m)
    for i in range(passes):
        template = env.from_string(doc)
        dictionary = yaml.load(doc)
        doc = template.render(dictionary)

    return yaml.load(doc)

def ensure_dir_exists(path, mode=0o777):
    """ensure that a directory exists
    If it doesn't exist, try to create it, protecting against a race condition
    if another process is doing the same.
    The default permissions are determined by the current umask.
    """
    try:
        os.makedirs(path, mode=mode)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
    if not os.path.isdir(path):
        raise IOError("%r exists but is not a directory" % path)

def validate(metadata, schema_filename):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    filename = os.path.abspath(os.path.join(dir_path, 'schema/{}'.format(schema_filename)))

    schema={}
    with open(filename) as f:
        schema = yaml.load(f)

    jsonschema.validate(metadata, schema)

def repo_data():
    try:
        repo = git.Repo(search_parent_directories=True)
        (commit, branch) = repo.head.object.name_rev.split(' ')
        msg = {
            'type': 'git',
            'committer': repo.head.object.committer.name,
            'hash': commit[:7],
            'commit': commit,
            'branch': branch,
            # How to get url
            'url': repo.remotes.origin.url,
            'name': repo.remotes.origin.url.split('/')[-1],
            # How to get humanable time
            'date': repo.head.object.committed_datetime.isoformat(),
            'clean': len(repo.index.diff(None))== 0
        }
    except:
        msg = {
            'type': None,
            'committer': '',
            'hash': 0,
            'commit': 0,
            'branch': '',
            # How to get url
            'url': '',
            'name':'',
            # How to get humanable time
            'date': '',
            'clean': False
        }
    return msg
