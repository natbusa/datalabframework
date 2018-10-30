import builtins
builtins.__DATALABFRAMEWORK__=True

from ._version import version_info, __version__

# automatically imports submodules
# by loading the package, as in `import datalabframework as dlf`

from . import logging
from . import notebook
from . import params
from . import data
from . import engines
from . import project
from . import utils


# from datalabframework import *
# imports the following according to the __all__ variable
__all__ = [
    'logging',
    'notebook',
    'project',
    'params',
    'data',
    'engines',
    'utils'
]
