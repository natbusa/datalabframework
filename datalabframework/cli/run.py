import os
import glob

from textwrap import dedent

import nbformat
from nbconvert.preprocessors import ExecutePreprocessor, CellExecutionError

from .application import DatalabframeworkApp

from traitlets import (
    Unicode, List, Dict
)


def preprocess_cell(self, cell, resources, cell_index):
    """
    Executes a single code cell. See base.py for details.
    To execute all cells see :meth:`preprocess`.
    """
    if cell.cell_type != 'code':
        return cell, resources

    print('Input cell: {}'.format(cell['source']))
    reply, outputs = self.run_cell(cell, cell_index)
    print('Output: {}'.format(outputs))
    cell.outputs = outputs

    if not self.allow_errors:
        for out in outputs:
            if out.output_type == 'error':
                raise CellExecutionError.from_cell_and_msg(cell, out)
        if (reply is not None) and reply['content']['status'] == 'error':
            raise CellExecutionError.from_cell_and_msg(cell, reply['content'])
    return cell, resources

class DlfRunApp(DatalabframeworkApp):
    name = Unicode(u'datalabframework-run')
    description = "Executing a datalabframework notebook"

    # classes
    classes = List([ExecutePreprocessor])

    # config
    config_file = Unicode(u'',help="Load this config file").tag(config=True)

    # app settings
    profile = Unicode(u'default', help="Execute a specific metadata profile").tag(config=True)
    rootdir = Unicode(os.getcwd(), help="project root directory").tag(config=True)

    notebooks = List([], help="""
        List of notebooks to convert. Wildcards are supported.
        Filenames passed positionally will be added to the list.
        """).tag(config=True)

    # aliases
    aliases = Dict({
        'profile': 'DlfRunApp.profile',
        'rootdir': 'DlfRunApp.rootdir',
        'timeout': 'ExecutePreprocessor.timeout',
        'notebooks': 'DlfRunApp.notebooks'})

    #flags
    flags = Dict()

    def init_preprocessor(self):
        self.ep = ExecutePreprocessor(config=self.config)

    def init_notebooks(self):
        if self.extra_args:
            patterns = self.extra_args
        else:
            patterns = self.notebooks

        # Use glob to replace all the notebook patterns with filenames.
        filenames = []
        for pattern in patterns:

            # Use glob to find matching filenames.  Allow the user to convert
            # notebooks without having to type the extension.
            globbed_files = glob.glob(pattern)
            globbed_files.extend(glob.glob(pattern + '.ipynb'))
            if not globbed_files:
                self.log.warning("pattern %r matched no files", pattern)

            for filename in globbed_files:
                if filename not in filenames:
                    filenames.append(filename)
        self.notebooks = filenames

    def notebook_statistics(self, data):
        # todo: check for filetype
        stats = {'cells': len(data['cells'])}

        h = dict()
        for c in data['cells']:
            count = h.get(c['cell_type'], 0)
            h[c['cell_type']] = count + 1
        stats.update(h)

        error = {'ename': None, 'evalue': None}
        for c in data['cells']:
            if c['cell_type'] == 'code':
                for o in c['outputs']:
                    if o['output_type'] == 'error':
                        error = {'ename': o['ename'], 'evalue': o['evalue']}
                        break
        stats.update(error)

        count = 0
        for c in data['cells']:
            if c['cell_type'] == 'code' and c['execution_count']:
                count += 1
        stats.update({'executed': count})

        return stats

    def initialize(self, argv=None):
        self.parse_command_line(argv)
        if self.config_file:
            self.load_config_file(self.config_file)
        self.init_notebooks()
        self.init_preprocessor()

    def run_single_notebook(self, filename):
        nb = nbformat.read(filename, as_version=4)
        filename_tuple = os.path.split(filename)

        fullpath_filename = os.path.join(os.getcwd(), *filename_tuple)
        cwd = os.path.dirname(fullpath_filename)
        init_str = dedent(f"""
            # added by dlf-run
            import datalabframework
            datalabframework.files.set_script_path('{fullpath_filename}')
            datalabframework.project.load(profile='{self.profile}', rootpath='{self.rootdir}')
            """)

        nc = nbformat.v4.new_code_cell(init_str)
        nb['cells'].insert(0, nc)

        resources = {}
        resources['metadata'] = {'path': os.getcwd()}

        (nb_out, resources_out) = self.ep.preprocess(nb, resources)

    def start(self):
        for notebook_filename in self.notebooks:
            self.run_single_notebook(notebook_filename)


def main():
    ExecutePreprocessor.preprocess_cell = preprocess_cell
    app = DlfRunApp()
    app.initialize()
    app.start()
