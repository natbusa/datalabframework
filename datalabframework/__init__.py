# automatically imports submodules
# by loading the package, as in `import datalabframework as dlf`

from . import logging
from . import notebook
from . import params
from . import data
from . import engines
from . import project
from . import utils
# local builtin global __DATALABFRAMEWORK__
# in the current python context (super global)
from . import _version

# expose version_info and version variables
from ._version import version_info, __version__

#from datalabframework import *
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
