import os
import sys
import getpass

from datalabframework import __version__

from datalabframework import paths
from datalabframework import files
from datalabframework import engine
from datalabframework import logging

from datalabframework.metadata import reader
from datalabframework.metadata import resource

from datalabframework._utils import Singleton, ImmutableDict, repo_data
from datalabframework._notebook import NotebookFinder

class Config(metaclass=Singleton):

    def __init__(self, filename=None, profile=None, wordir_path=None, dotenv_path=None):

        #object variablesF
        self._initialized = False
        self._profile = None
        self._metadata = None
        self._dotenv_path = None
        self._info = None
        self._engine = engine.NoEngine()

        # set filename for future references
        files.set_current_file(filename)

        # set workdir and load metadata
        self.load(profile, wordir_path, dotenv_path)

    def load(self, profile=None, wordir_path=None, dotenv_path=None):

        # init workdir and rootdir paths
        paths.workdir(wordir_path)

        # set dotenv default file
        if dotenv_path is None:
            dotenv_path = os.path.join(paths.rootdir(), '.env')

        # if file is valid and exists, store in the object context.
        if os.path.isfile(dotenv_path):
            self._dotenv_path = os.path.abspath(dotenv_path)

        # metadata files
        metadata_files = files.get_metadata_files(paths.rootdir())

        # load metadata
        md = reader.load(
            profile,
            [ os.path.join(paths.rootdir(), x) for x in metadata_files],
            self._dotenv_path)

        # validate metadata
        reader.validate(md)

        # store metadata in project opbject
        self._metadata = md

        # set profile, only if not set yet
        self._profile = self._metadata['profile']

        # add roothpath to the list of python sys paths
        if paths.rootdir() not in sys.path:
            sys.path.append(paths.rootdir())

        # register hook for loading ipynb files
        if 'NotebookFinder' not in str(sys.meta_path):
            sys.meta_path.append(NotebookFinder())

        # initialize the engine
        repo_name = repo_data()['name']
        jobname = '{}-{}'.format(self._profile, repo_name) if repo_name else self._profile

        # stop existing engine
        if self._engine:
            self._engine.stop()

        self._engine = engine.get(jobname, self._metadata, paths.rootdir())

        # get all project info
        self._info = {
                'version': __version__,
                'profile': self._profile,
                'filename': os.path.relpath(files.get_current_file(), paths.rootdir()),
                'rootdir': paths.rootdir(),
                'workdir': paths.workdir(),
                'username': getpass.getuser(),
                'repository': repo_data(),
                'files': {
                    'notebooks': files.get_jupyter_notebook_files(paths.rootdir()),
                    'python': files.get_python_files(paths.rootdir()),
                    'metadata': files.get_metadata_files(paths.rootdir()),
                    'dotenv': os.path.relpath(self._dotenv_path, paths.rootdir()) if self._dotenv_path else None,
                },
                'engine': self._engine.config().to_dict()
        }

        # initialize logging
        logging.init()

    def config(self):
        return ImmutableDict(self._info)

    def profile(self):
        return self._profile

    def metadata(self):
        return ImmutableDict(self._metadata)

    def resource(self, path=None, provider=None, md=dict()):
        md = resource.get_metadata(paths.rootdir(), self._metadata , path, provider, md)
        return ImmutableDict(md)

    def engine(self):
        return self._engine
