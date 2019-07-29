import os
import re
import ctypes
import zlib
import functools

from urllib.parse import urlparse
from collections import namedtuple
from copy import deepcopy

from datalabframework import metadata
from datalabframework.paths import rootdir
from datalabframework._utils import merge, to_ordered_dict
from datalabframework.yaml import YamlDict

from datalabframework.download import download

import datalabframework.logging as log

Urn = namedtuple('Urn', ['scheme', 'user', 'password', 'host', 'port', 'path', 'params', 'query', 'fragment'])

def filter_empty(lst):
    return [x for x in lst if x != '' and x is not None]

def tsplit(s, sep, shift='left'):
    s = s.split(sep)
    if shift=='left':
        return (s[0],s[1]) if len(s)>1 else ('', s[0])
    else: # right
        return (s[0],s[1]) if len(s)>1 else (s[0], '')
    
def urnparse(s):
    scheme, url = tsplit(s, '//')

    path_only = ((not scheme) or 
        scheme.startswith('jdbc:sqlite') or
        scheme.startswith('s3a'))        
        
    url = urlparse(url) if path_only else urlparse('scheme://'+url)

    path = url.path
    query = url.query
    
    scheme = filter_empty(scheme.split(':'))
    
    auth, netloc = tsplit(url.netloc, '@')
    user, password = tsplit(auth, ':', 'right')
    host, port = tsplit(netloc,':', 'right') 
    
    # parsing oracle thin urn for user, password
    oracle_thin_scheme = len(scheme)==4 and ':'.join(scheme[0:3])=='jdbc:oracle:thin'
    if oracle_thin_scheme and scheme[-1][-1]=='@':
        o_user, o_password = tsplit(scheme[-1].rstrip('@'), '/', 'right')
        user = o_user or user
        password = o_password or password
    
    # parsing oracle params
    if oracle_thin_scheme:
        path, *params = path.split(',')
        query = query + '&'.join(filter_empty(params))
        
    # parsing mssql params
    jdbc_mssql_scheme = len(scheme)==2 and ':'.join(scheme[0:2])=='jdbc:sqlserver'
    if jdbc_mssql_scheme:
        netloc, *params = netloc.split(';')
        host, port = tsplit(netloc,':', 'right') 
        query = query + '&'.join(filter_empty(params))
    
    params = filter_empty(query.split('&'))
    params = [tuple(p.split('=')) for p in params]
    
    urn = Urn(scheme,user, password, host, port, path, params, query, url.fragment)
    return urn 

def path_to_jdbc(md, provider=False):
    
    database = md['database']
    table = md['table']
    path = md['path'] or ''
    
    if md['format']!='jdbc':
        return database, table, path
    
    e = filter_empty(path.split('/'))
    
    if len(e)==0:
        pass;
    elif len(e)==1:
        if provider:
            database = e[0] or None
            path = None
        else:
            table = e[0] or None
            path = None
    else:    
        database = e[0] or None
        table = e[1] or None
        path = None
        
    return database, table, path

def get_default_md():
    f = [
        'service',
        'format',
        
        'host',
        'port',

        'driver', 
        'database', 
        'schema', 
        'table', 
        
        'user',
        'password',
        
        'path', 
        'options',
        
        'provider'
    ]

    return dict(zip(f, [None for _ in range(len(f))]))

def metadata_overrides(md, host=None, service=None, port=None, user=None, password=None,
                driver=None, database=None, schema=None, table=None, format=None, 
                hostname=None, username=None, **options):
    
    d = {}
    d['path'] = md.get('url') or md.get('path')
    d['provider'] = md.get('provider')
    
    d['host'] = host or hostname or md['host'] or md.get('hostname')
    d['port'] = port or md['port']

    d['service'] = service or md['service']
    d['format'] = format or md['format']

    d['user'] =  user or username or md['user'] or md.get('username')
    d['password'] =  password or md['password']
    
    d['database'] =  database or md['database']
    d['schema'] =  schema or md['schema']
    d['table'] = table or md['table']
    d['driver'] =  driver or md['driver']
    d['options'] = merge(md['options'], options)
    
    if database or table:
        d['path'] = None
    
    return d

def resource_from_dict(d):
    md = get_default_md()
    d['path'] = d.get('path') or d.get('url')
    for k in md.keys():
        md[k] = d.get(k)
    return md

def resource_from_urn(urn):
    
    md = get_default_md()
    query = get_sql_query(urn.path)
    if query:
        md['table'] = query
        md['format'] = 'jdbc'
        return md
    
    params = dict(urn.params)
    
    if urn.scheme and urn.scheme[0]=='jdbc':
        service, format = urn.scheme[1], urn.scheme[0]        
    else:
        service = urn.scheme[0] if urn.scheme else ''
        format = get_format({'format':None, 'service': service, 'path': urn.path})
        
        compression = get_compression(urn.path)
        if compression:
            params['compression'] = compression
    
    md['service'] = service
    md['format'] = format
    md['host'] = urn.host
    md['port'] = urn.port

    md['path'] = urn.path

    md['user'] = urn.user
    md['password'] = urn.password

    md['options'] = params
    
    for k,v in md.items():
        if not v:
            md[k] = None
    
    return md

def get_sql_query(s):
    # if SQL query is detected, 
    # wrap the resource path as a temp table
    sql_query = s
    sql_query = sql_query.replace('\n', ' ')
    sql_query = sql_query.replace('\t', ' ')
    sql_query = sql_query.replace('\r', ' ')
    sql_query = ' '.join(sql_query.split())
    sql_query = sql_query.rstrip(' ')
    sql_query = sql_query.rstrip(';')
    sql_query = sql_query.lower()

    #imple sql test: check for from or where prefixed with a space
    # indicates multiple words
    if any([x in sql_query for x in [' from ', ' where ']]):
        return sql_query
    else:
        return None

def to_resource(url_alias=None, *args, **kwargs):
    
    md = None
    
    # if a dict, create from dictionary
    if isinstance(url_alias, dict):
        md = resource_from_dict(url_alias)
    
    # if a string, and a metadata profile is loaded, check for aliases
    if metadata.profile():
        if not md and url_alias in metadata.profile().get('resources', {}).keys():
            md = metadata.profile()['resources'][url_alias]
        
        if not md and url_alias in metadata.profile().get('providers', {}).keys():
            md = metadata.profile()['providers'][url_alias]
    
    # if nothing found yet, interpret as a urn/path
    if not md and url_alias:
        md = resource_from_urn(urnparse(url_alias))
        
    # empty default
    if not md:
        md = get_default_md()

    # sanitize path if it's a url or a query
    if md['path']:
        url_md = resource_from_urn(urnparse(md['path']))
        md = merge(url_md, md)
        md['path'] = url_md['path']

    # override using kwargs
    md = metadata_overrides(md, **kwargs)

    if 'hostname' in md:
        del md['hostname']

    if 'username' in md:
        del md['username']
    
    return md

def get_compression(path):
    if not path:
        return None
    
    _, ext = os.path.splitext(path)
    d = {
        '.gz':'gzip', 
        '.bz2':'bzip2',
    }
    return d.get(ext)
     
def get_format(md):
    
    if md['format']:
        return md['format']
    
    # get the provider format
    if md['service'] in ['hdfs']:
        return 'parquet'

    if md['service'] in ['sqlite', 'mysql', 'postgres', 'mssql', 'oracle']:
        return 'jdbc'

    if md['service'] in ['elastic']:
        return 'json'

    # check if path is a query
    query = get_sql_query(md['path'])
    if query:
        return 'jdbc'
    
    # extract the format from file extension
    #‘.gz’, ‘.bz2’, ‘.zip’, ‘.snappy’, '.deflate'
    path, ext = os.path.splitext(md['path'])
    if get_compression(md['path']):
        _, ext = os.path.splitext(path)
    
    if ext and ext[0]=='.':
        ext = ext[1:]
        
    # default is None
    return ext or None

def get_driver(service):
    drivers = {
        'sqlite': 'org.sqlite.JDBC',
        'mysql': 'com.mysql.cj.jdbc.Driver',
        'postgres': 'org.postgresql.Driver',
        'mssql': 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
        'oracle': 'oracle.jdbc.driver.OracleDriver'
    }
    return drivers.get(service)

def get_port(service):
    ports = {
        'hdfs': 8020,
        'mysql': 3306,
        'postgres': 5432,
        'mssql': 1433,
        'oracle': 1521,
        'elastic': 9200,
        's3a':9000
    }
    return ports.get(service)

def get_url(md):
    service = md['service']
    path = md['path']
    
    host_port = f"{md['host']}:{md['port']}" if md['port'] else md['host']
    
    if  service in ['local', 'file']:
        url = path
    elif service == 'sqlite':
        url = f"jdbc:sqlite:{path}"
    elif service == 'hdfs':
        url = f"hdfs://{host_port}{md['path']}"
    elif service in ['http', 'https']:
        url = f"{service}://{host_port}{md['path']}"
    elif service in ['minio', 's3a']:
        url = f"s3a://{md['path']}'"
    elif service == 'mysql':
        url = f"jdbc:mysql://{host_port}/{md['database']}"
    elif service == 'postgres':
        url = f"jdbc:postgresql://{host_port}/{md['database']}"
    elif service == 'mssql':
        url = f"jdbc:sqlserver://{host_port};databaseName={md['database']}"
    elif service == 'oracle':
        url = f"jdbc:oracle:thin:@//{host_port}/{md['database']}"
    elif service == 'elastic':
        url = f"http://{host_port}/{md['database']}"

    return url


def process_metadata(md):
    
    
    # update format from 
    md['format'] = get_format(md)
    
    # if no service, at this point use file
    md['service'] = md['service'] or 'file'
    
    # standardize some service names
    services = {
        'minio': 's3a',
        'local': 'file'
    }
    md['service'] = services.get(md['service'], md['service'])
    
    # if no host, use localhost
    md['host'] = md['host'] or '127.0.0.1'
        
    # if local file system and rel path, prepend rootdir
    if md['service'] in ['file', 'sqlite'] and not os.path.isabs(md['path']):
        md['path'] = os.path.join(rootdir(), md['path'])

    # if service is s3a, remove leading '/'
    if md['service'] == 's3a' and md['path']:
        md['path'] = md['path'].lstrip('/')

    # generate database, table from path
    if md['format']=='jdbc':
        md['database'], md['table'], md['path'] = path_to_jdbc(md)

        # set driver
        md['driver'] = md['driver'] or get_driver(md['service'])
        
        # if not table, provide no result query
        md['table'] = md['table'] or 'SELECT 0 as result where 1 = 0'
        
        # if schema is not yet defined, 
        # take the default for each service
        default_schemas = {
            'mysql': md['database'],
            'mssql': 'dbo',
            'postgres': 'public',
            'oracle': md['user']
        }
        
        md['schema'] = md['schema'] or default_schemas.get(md['service'])
        
        query = get_sql_query(md['table'])
        if query and not query.endswith('as _query'):
            md['table'] = '( {} ) as _query'.format(query)

    md['port'] = md['port'] or get_port(md['service'])
    md['port'] = int(md['port']) if md['port'] else None
    md['url'] = get_url(md)

    if not isinstance(md['options'], dict):
        md['options'] = {}
    
    compression = get_compression(md['path'])
    if md['format']!='jdbc' and compression:
        md['options']['compression'] = compression
    
    h_list = []
    for k in ['url', 'format', 'table', 'database']:
        v = zlib.crc32(md[k].encode()) if md[k] else 0
        h_list.append(v)
        
    md['hash'] = functools.reduce(lambda a,b : a^b, h_list)
    md['hash'] = hex(ctypes.c_size_t(md['hash']).value)
    
    return md

def assemble_metadata(md):
    keys = [
        'hash',
        'url',
        'service',
        'format',
        
        'host'
    ]
    
    if md['service'] != 'file':
        keys.append('port')

    if md['service'] == 's3a' or md['format'] == 'jdbc':
        keys.extend([
            'user',
            'password'])
        
    if md['format'] == 'jdbc':
        keys.extend([
            'driver',
            'database',
            'schema',
            'table'])
        
    keys.append('options')
    return YamlDict(to_ordered_dict(md, keys))
        
def Resource(path_or_alias_or_url=None, provider_path_or_alias_or_url=None, 
        host=None, service=None, port=None, user=None, password=None,
        driver=None, database=None, schema=None, table=None, format=None, 
        hostname=None, username=None, **options):

    prov = provider_path_or_alias_or_url
    path = path_or_alias_or_url
    
    # get the resource, by alias metadata or by url
    rmd = to_resource(path, host=host, service=service, port=port, 
        user=user, password=password, driver=driver, database=database, 
        schema=schema, table=table, format=format, hostname=hostname, 
        username=username, **options)
    
    # get the provider by reference from the resource, if available
    prov = prov or rmd.get('provider')
    
    # get the provider, by alias metadata or by url
    pmd = to_resource(prov)
    
    # check if the provider is a jdbc connection, if so set it
    pmd['database'], pmd['table'], pmd['path'] = path_to_jdbc(pmd, True)

    # merge provider and resource metadata
    md = merge(pmd,rmd)
    
    
    # concatenate paths, if no table is defined
    if md['table']:
        md['path'] = None
    else:
        md['path'] = os.path.join(pmd['path'] or '', rmd['path'] or '')

    #process metadata
    md = process_metadata(md)
    
    #todo: verify resource
    # check format and other minimum requirements are met
    
    # assemble output
    md = assemble_metadata(md)

    return md

def get_local(md):
    if md['service'].startswith('http'):
        md['path']  = download(md['url'], md['format'])
        md['service'] = 'file'
        md['url'] = None
        return Resource(md)
    else:
        return md
