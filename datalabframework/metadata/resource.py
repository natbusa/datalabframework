import os
import ctypes

from datalabframework._utils import merge, to_ordered_dict

def _url(d):

    service = d['service']
    fullpath = os.path.join(d['provider_path'],d['resource_path'])

    if  service in ['local', 'file']:
        url = os.path.realpath(fullpath)
    elif service == 'sqlite':
        url = 'jdbc:sqlite:{}'.format(os.path.realpath(fullpath))
    elif service == 'hdfs':
        url = 'hdfs://{}:{}{}'.format(d['host'], d['port'], fullpath)
    elif service == 'minio':
        url = 's3a://{}'.format(fullpath)
    elif service == 'mysql':
        url = 'jdbc:mysql://{}:{}/{}'.format(d['host'],d['port'], d['database'])
    elif service == 'postgres':
        url = 'jdbc:postgresql://{}:{}/{}'.format(d['host'], d['port'], d['database'])
    elif service == 'mssql':
        url = 'jdbc:sqlserver://{}:{};databaseName={}'.format(d['host'], d['port'], d['database'])
    elif service == 'oracle':
        url = 'jdbc:oracle:thin:@//{}:{}/{}'.format(d['host'], d['port'], d['database'])
    elif service == 'elastic':
        url = 'http://{}:{}/{}'.format(d['host'], d['port'], d['database'])
    else:
        url = None

    return url

def _port(service_name):
    ports = {
        'hdfs': 8020,
        'mysql': 3306,
        'postgres': 5432,
        'mssql': 1433,
        'oracle': 1521,
        'elastic': 9200,
        'minio':9000
    }
    return ports.get(service_name)

def _format(d):

    # get the provider format
    if d.get('service') in ['sqlite', 'mysql', 'postgres', 'mssql', 'oracle']:
        return 'jdbc'

    if d.get('service') in ['elastic', 'mongodb']:
        return 'nosql'

    path = d.get('resource_path', '').split('.')
    if len(path)>1 and path[-1] in ['csv', 'json', 'jsonl']:
        return path[-1]

    path = d.get('provider_path', '').split('.')
    if len(path)>1 and path[-1] in ['csv', 'json', 'jsonl']:
        return path[-1]

    # default is parquet
    return d.get('format', 'parquet')

def _driver(d):
    drivers = {
        'sqlite': 'org.sqlite.JDBC',
        'mysql': 'com.mysql.cj.jdbc.Driver',
        'postgres': 'org.postgresql.Driver',
        'mssql': 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
        'oracle':'oracle.jdbc.driver.OracleDriver'
    }
    return drivers.get(d.get('service'))


def _get_resource_metadata(metadata=dict(), resource=None, provider=None):

    if 'resources' not in metadata.keys():
        metadata['resources'] = {}

    #first match by resource alias
    rmd = metadata['resources'].get(resource, {})
    if rmd:
        rmd['alias'] = resource
        if provider:
            rmd['provider'] = provider
        if not rmd.get('path'):
            rmd['path'] = resource

    # match resource not as alias but as a path in any of the available resources,
    # using the given provider name and provider name
    if not rmd:
        for resource_alias in metadata['resources'].keys():
            resource_candidate = metadata['resources'][resource_alias]
            if  resource_candidate.get('path') and resource_candidate.get('path') == resource and \
                    resource_candidate.get('provider') and resource_candidate.get('provider') == provider:
                rmd = resource_candidate
                rmd['alias'] = resource_alias
                break

    # if nothing yet, try with path alone
    if not rmd:
        for resource_alias in metadata['resources'].keys():
            resource_candidate = metadata['resources'][resource_alias]
            if resource_candidate.get('path') and resource_candidate.get('path') == resource:
                rmd = resource_candidate
                rmd['alias'] = resource_alias
                if provider:
                    rmd['provider'] = provider
                break

    #still nothing use path and provider as minimal resource info
    if not rmd:
        if provider:
            rmd['provider'] = provider

        # if resource is given use it as a path
        if resource:
            rmd['path'] = resource

    # nothing found, return None
    return rmd

def _get_provider_metadata(metadata=dict(), rmd=None):
    #if no resource dictionary, return an empty dictionary
    if not rmd:
        return {}

    provider = rmd.get('provider')
    providers = metadata.get('providers', {})

    pmd = {}
    if provider:
        if provider in providers.keys():
            pmd = providers.get(provider, {})
            pmd['alias'] = provider
        else:
            # if no valid provider alias at this point, use provider as a path
            pmd = {'path':provider, 'alias': None}

    return pmd

def _override_metadata(access, param, pmd=dict(), rmd=dict()):
    d = merge(pmd.get(access, {}).get(param, {}), rmd.get(access, {}).get(param, {}))
    return d

def _build_resource_metadata(rootdir, pmd={}, rmd={}, user_md=dict()):

    d = merge(pmd, rmd)
    
    d['provider_path'] = pmd.get('path', '')
    d['resource_path'] = rmd.get('path', '')
    d['provider_alias'] = pmd.get('alias')
    d['resource_alias'] = rmd.get('alias')
    d.pop('alias', None)
    d.pop('path', None)
    
    d['rootdir'] = rootdir

    if not d.get('service'):
        parts = d['provider_path'].split('://')
        if len(parts)>1:
            d['service'] = parts[0]
            d['provider_path'] = parts[1]

    if not d.get('service'):
        parts = d['resource_path'].split('://')
        if len(parts)>1:
            d['service'] = parts[0]
            d['resource_path'] = parts[1]

    if not d.get('service'):
        d['service'] = 'file'

    # if service is local or sqlite,
    # relative path is allowed, and prefixed with rootpath
    if d['service'] in ['file', 'sqlite'] and \
            not os.path.isabs(d['provider_path']) and \
            not os.path.isabs(d['resource_path']):
        d['provider_path'] = os.path.realpath(os.path.join(d['rootdir'], d['provider_path']))

    d['format'] = _format(d)
    d['driver'] = _driver(d)

    #default hostname is localhost
    d['host'] = d.get('hostname', d.get('host','127.0.0.1'))

    # provider path can be use as database name
    if not d.get('database') and d.get('format') in ['jdbc', 'nosql']:
        d['database'] = d.get('path')

    d['port'] = d.get('port', _port(d['service']))
    d['url'] = _url(d)

    d['options'] = d['options'] if d.get('options') else {}
    d['mapping'] = d['mapping'] if d.get('mapping') else {}
    
    # override with function provided metadata
    d = merge(d, user_md)
    
    d['hash'] = hash(d['url']) ^ hash(d['format']) ^ hash(d['resource_path'])
    d['hash'] = hex(ctypes.c_size_t(d['hash']).value)

    return d

def _dict_formatting(d):
    keys = (
        'hash',
        'url',
        'service',
        'format',
        
        'host',
        'port',

        'driver',
        'database',
        'username',
        'password',
        'resource_path',
        'provider_path',
        
        'provider_alias',
        'resource_alias',
        
        'cache',
        'date_column',  
        'date_start',
        'date_end',
        'date_window',
        'date_partition',
        'update_column',
        'hash_column',
        'state_column',

        'options',
        'mapping',
    )

    return to_ordered_dict(d, keys)

def get_metadata(rootdir=None, metadata=dict(), path=None, provider=None, md=dict()):
    """
    :param metadata: resources metadata
    :param rootdir:  directory path for relative local files
    :param path: name of the resource alias or resource path
    :param provider:  name of the provider alias
    :param md: dictionary of metadata, overrides provider and resource metadata
    :return: None or a dictionary with the resource propertiees:
    """
    rootdir = rootdir if rootdir else os.getcwd()

    rmd = _get_resource_metadata(metadata, path, provider)
    pmd = _get_provider_metadata(metadata, rmd)

    d = _build_resource_metadata(rootdir, pmd, rmd, md)
    return _dict_formatting(d)
