from datalabframework._project import Config

def load(profile=None, workdir=None, dotenv_path=None):
    """
    Performs the following steps:
      - set workdir and rootdir for the given project
      - perform .env env variable exporting,
      - Load the given profile from the metadata files,

    Note that:
     1 ) rootdir can search the upper directories of workdir,
         rootdir is detected by the presence of a __main__.py file
         in no __main__.py is detected, the current working dir is taken as root dir for the project

     2) metadata files are merged up, so you can split the information in multiple files as long as they end with metadata.yml
        metadata.yml, abc.metadata.yaml, abc_'function' object is not subscriptablemetadata.yml or all valid metadata file names
        all metadata files in all subdirectories from the project root directory are loaded

     3) all metadata profiles inherit the settings from profile 'default'

    :param profile: load the given metadata profile (default: 'default')
    :param workdir: change to the given working directory (default: current working dir)
    :param dotenv_path: load variable from a dotenv file (default: <rootdir>/.env)
    :return:
    """
    c = Config(profile=profile, workdir=workdir, dotenv_path=dotenv_path)

def config():
    """
    Returns the current project configuration
    :return: a dictionary with project configuration data
    """
    return Config().config()

def engine():
    """
    Start up a session according to the engine settings in the loaded metadata profile
    :param name: the alias of the engine as defined in the metadata configuration profile
    :return:
    """
    return Config().engine()

def metadata():
    """
    return a metadata object which provides just one method:
    config() : provides the current loaded metadata profile information

    :return: a Metadata object
    """
    return Config().metadata()

def resource(path=None, provider=None, md=dict()):
    """
    returns a resource object for read and write operations
    This object provides a config() method which returns the dictionary

    :param path: the path or the alias of the resource
    :param provider: as defined in the current metadata profile
    :param md: dictionary override
    :return: a Resouce Object
    """
    return Config().resource(path, provider, md)