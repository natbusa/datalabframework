from . import params
from . import project
from . import utils

import os

def uri(resource):
    return params.resource_unique_name(resource, project.filename(False))

def _url(md):
    pmd = md['provider']
    rmd = md['resource']

    #defaults
    if  pmd['service'] == 'local':
        pmd['path'] = pmd.get('path',project.rootpath())
        if pmd['path'][0]!='/':
            pmd['path'] = '{}/{}'.format(project.rootpath(), pmd['path'])
            pmd['path'] = os.path.abspath(pmd['path'])
    else:
        pmd['path'] = pmd.get('path','')

    pmd['hostname'] = pmd.get('hostname', '127.0.0.1')
    pmd['path'] = utils.lrchop(pmd['path'], '/')
    rmd['path'] = rmd.get('path','')

    fullpath = os.path.join(pmd['path'],rmd['path'])

    if  pmd['service'] == 'local':
        url = "file:///{}".format(fullpath)
    elif pmd['service'] == 'hdfs':
        url = "hdfs://{}:{}/{}".format(pmd['hostname'],pmd.get('port', '8020'),fullpath)
    elif pmd['service'] == 'minio':
        url = "s3a:///{}".format(fullpath)
    elif pmd['service'] == 'sqlite':
        url = "jdbc:sqlite:" + pmd['path']
    elif pmd['service'] == 'mysql':
        url = "jdbc:mysql://{}:{}/{}".format(pmd['hostname'],pmd.get('port', '3306'),pmd['database'])
    elif pmd['service'] == 'postgres':
        url = "jdbc:postgresql://{}:{}/{}".format(pmd['hostname'],pmd.get('port', '5432'),pmd['database'])
    elif pmd['service'] == 'mssql':
        url = "jdbc:sqlserver://{}:{};databaseName={}".format(pmd['hostname'],pmd.get('port', '1433'),pmd['database'])
    elif pmd['service'] == 'oracle':
        url = "jdbc:oracle:thin:{}/{}@//{}:{}/{}".format(pmd['username'], pmd['password'], pmd['hostname'],pmd.get('port', '1521'), pmd['sid'])
    elif pmd['service'] == 'elastic':
        url = 'http://{}:{}/{}'.format(pmd["hostname"], pmd.get("port", 9200), rmd['path'])
    else:
        url = None

    return url

def _get_resource_metadata(path=None, provider=None):
    md = params.metadata()

    if provider:
        for resource_name in md['resources'].keys():
            resource = md['resources'][resource_name]
            if str(resource.get('path')) == path and \
               str(resource.get('provider')) == provider:
                return resource

    # if nothing yet, try with path alone
    for resource_name in md['resources'].keys():
        resource = md['resources'][resource_name]
        if str(resource.get('path')) == path:
            return resource

    #still nothing use path and provider as minimal resource info
    path = path if path else md['resources'].get(provider, {}).get('path')
    return { 'path':path, 'provider':provider } if path else None

def metadata(resource=None, path=None, provider=None):
    md = params.metadata()

    # get the resource, either from resource name or path+provider
    rmd = md['resources'].get(uri(resource)) if resource else _get_resource_metadata(path, provider)

    # no sufficient info to construct a valid resource
    if not rmd:
        print('Resource not found: must specify either path and provider alias, or the resource alias')
        print('Debug: resource={}, path={}, provider={}'.format(resource, path, provider))
        return None

    # check consistency in metadata
    if rmd.get('provider') not in md['providers']:
        #print("resource provider '{}' not in the list of known providers".format(rmd['provider']))
        rmd['provider'] = provider

    # check consistency in metadata
    if provider and rmd['provider'] and rmd['provider'] != provider:
        print("Using the provider '{}' and instead of resource provider '{}'".format(provider, rmd['provider']))
        rmd['provider'] = provider

    # get the provider
    pmd =  md['providers'].get(rmd['provider'])

    if not pmd:
        print('Provider not found: must specify either path and provider alias, or the resource alias')
        print('Debug: resource={}, path={}, provider={}'.format(resource, path, provider))
        return None

    #connstruct resource
    d = {'resource': rmd, 'provider':pmd}

    #cleanup
    if 'path' in d['resource']:
        d['resource']['path'] = str(d['resource']['path'])

    if 'path' in d['provider']:
        d['provider']['path'] = str(d['provider']['path'])

    # augment resource metadata
    d['url'] = _url(d)

    #that's all
    return d
