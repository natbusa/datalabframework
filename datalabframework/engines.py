import os

from jinja2 import Template

from . import params
from . import data
from . import utils
from . import project
import elasticsearch.helpers
import json
from datetime import date, timedelta, datetime
import pyspark
from pyspark.sql.functions import desc, lit

from . import logging
logger = logging.getLogger()

# purpose of engines
# abstract engine init, data read and data write
# and move this information to metadata

# it does not make the code fully engine agnostic though.

engines = dict()

class SparkEngine():
    def __init__(self, name, config):
        from pyspark import SparkContext, SparkConf
        from pyspark.sql import SQLContext

        here = os.path.dirname(os.path.realpath(__file__))

        submit_args = ''

        jars = []
        jars += config.get('jars', [])
        if jars:
            submit_jars = ' '.join(jars)
            submit_args = '{} --jars {}'.format(submit_args, submit_jars)

        packages = config.get('packages', [])
        if packages:
            submit_packages = ','.join(packages)
            submit_args = '{} --packages {}'.format(submit_args, submit_packages)

        submit_args = '{} pyspark-shell'.format(submit_args)

        # os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages org.postgresql:postgresql:42.2.5 pyspark-shell"
        os.environ['PYSPARK_SUBMIT_ARGS'] = submit_args
        print('PYSPARK_SUBMIT_ARGS: {}'.format(submit_args))

        conf = SparkConf()
        if 'jobname' in config:
            conf.setAppName(config.get('jobname'))

        rmd = params.metadata()['providers']
        for v in rmd.values():
            if v['service'] == 'minio':
                conf.set("spark.hadoop.fs.s3a.endpoint", 'http://{}:{}'.format(v['hostname'],v.get('port',9000))) \
                    .set("spark.hadoop.fs.s3a.access.key", v['access']) \
                    .set("spark.hadoop.fs.s3a.secret.key", v['secret']) \
                    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                    .set("spark.hadoop.fs.s3a.path.style.access", True)
                break

        #set master
        conf.setMaster(config.get('master', 'local[*]'))

        #set log level fro spark
        sc = SparkContext(conf=conf)

        # pyspark set log level method
        # (this will not suppress WARN before starting the context)
        sc.setLogLevel("ERROR")

        self._ctx = SQLContext(sc)
        self.info = {'name': name, 'context':'spark', 'config': config}

    def context(self):
        return self._ctx

    def read(self, resource=None, path=None, provider=None, **kargs):
        md = data.metadata(resource, path, provider)
        if not md:
            print('no valid resource found')
            return

        utils.pretty_print(md)
        pmd = md['provider']
        rmd = md['resource']

        url = md['url']

        cache = pmd.get('read',{}).get('cache', False)
        cache = rmd.get('read',{}).get('cache', cache)

        repartition = pmd.get('read',{}).get('repartition', None)
        repartition = rmd.get('read',{}).get('repartition', repartition)

        coalesce = pmd.get('read',{}).get('coalesce', None)
        coalesce = rmd.get('read',{}).get('coalesce', coalesce)

        # override options on provider with options on resource, with option on the read method
        options = utils.merge(pmd.get('read',{}).get('options',{}), rmd.get('read',{}).get('options',{}))
        options = utils.merge(options, kargs)

        if pmd['service'] in ['sqlite', 'mysql', 'postgres', 'mssql']:
            format = pmd.get('format', 'rdbms')
        else:
            format = pmd.get('format', 'parquet')

        if pmd['service'] in ['local', 'hdfs', 'minio']:
            if format=='csv':
                obj= self._ctx.read.csv(url, **options)
            if format=='json':
                obj= self._ctx.read.option('multiLine',True).json(url, **options)
            if format=='jsonl':
                obj= self._ctx.read.json(url, **options)
            elif format=='parquet':
                obj= self._ctx.read.parquet(url, **options)

        elif pmd['service'] == 'sqlite':
            driver = "org.sqlite.JDBC"
            obj =  self._ctx.read \
                .format('jdbc') \
                .option('url', url)\
                .option("dbtable", rmd['path'])\
                .option("driver", driver)\
                .load(**options)

        elif pmd['service'] == 'mysql':
            driver = "com.mysql.cj.jdbc.Driver"
            obj =  self._ctx.read\
                .format('jdbc')\
                .option('url', url)\
                .option("dbtable", rmd['path'])\
                .option("driver", driver)\
                .option("user",pmd['username'])\
                .option('password',pmd['password'])\
                .load(**options)
        elif pmd['service'] == 'postgres':
            driver = "org.postgresql.Driver"
            obj =  self._ctx.read\
                .format('jdbc')\
                .option('url', url)\
                .option("dbtable", rmd['path'])\
                .option("driver", driver)\
                .option("user",pmd['username'])\
                .option('password',pmd['password'])\
                .load(**options)
        elif pmd['service'] == 'mssql':
            driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
            obj = self._ctx.read \
                .format('jdbc') \
                .option('url', url) \
                .option("dbtable", rmd['path']) \
                .option("driver", driver) \
                .option("user", pmd['username']) \
                .option('password', pmd['password']) \
                .load(**options)
        elif pmd['service'] == 'oracle':
            url = "jdbc:oracle:thin:{}/{}@//{}:{}/{}".format(pmd['username'], pmd['password'], pmd['hostname'],
                                                             pmd.get('port', '1521'), pmd['sid'])
            print(url)
            print(pmd['database'])
#             print(rmd['path'])
            driver = "oracle.jdbc.driver.OracleDriver"
            return self._ctx.read.format('jdbc').option('url', url) \
                .option("dbtable", "{}.{}".format(pmd['database'], rmd['path']).option("driver", driver) \
                .load(**options)
        elif pmd['service'] == 'elastic':
            # uri = 'http://{}:{}/{}'.format(pmd["hostname"], pmd["port"], md['path'])
            # print(options)
            obj = self.elastic_read(url, options.get('query', {}))
        else:
            raise('downt know how to handle this')

        obj = obj.repartition(repartition) if repartition else obj
        obj = obj.coalesce(coalesce) if coalesce else obj
        obj = obj.cache() if cache else obj

        #logging
        logger.info({'format':format,'service':pmd['service'],'path':rmd['path'], 'url':md['url']}, extra={'dlf_type':'engine.read'})

        return obj

    def write(self, obj, resource=None, path=None, provider=None, **kargs):
        md = data.metadata(resource, path, provider)
        if not md:
            print('no valid resource found')
            return

        pmd = md['provider']
        rmd = md['resource']

        url = md['url']

        cache = pmd.get('read',{}).get('cache', False)
        cache = rmd.get('read',{}).get('cache', cache)

        repartition = pmd.get('write',{}).get('repartition', None)
        repartition = rmd.get('write',{}).get('repartition', repartition)

        coalesce = pmd.get('write',{}).get('coalesce', None)
        coalesce = rmd.get('write',{}).get('coalesce', coalesce)

        # override options on provider with options on resource, with option on the read method
        options = utils.merge(pmd.get('write',{}).get('options',{}), rmd.get('write',{}).get('options',{}))
        options = utils.merge(options, kargs)

        obj = obj.repartition(repartition) if repartition else obj
        obj = obj.coalesce(coalesce) if coalesce else obj
        obj = obj.cache() if cache else obj

        if pmd['service'] in ['sqlite', 'mysql', 'postgres', 'mssql']:
            format = pmd.get('format', 'rdbms')
        else:
            format = pmd.get('format', 'parquet')

        if pmd['service'] in ['local', 'hdfs', 'minio']:
            if format=='csv':
                obj.write.csv(url, **options)
            if format=='json':
                obj.write.option('multiLine',True).json(url, **options)
            if format=='jsonl':
                obj.write.json(url, **options)
            elif format=='parquet':
                obj.write.parquet(url, **options)
            else:
                print('format unknown')

        elif pmd['service'] == 'sqlite':
            driver = "org.sqlite.JDBC"
            obj.write\
                .format('jdbc')\
                .option('url', url)\
                .option("dbtable", rmd['path'])\
                .option("driver", driver)\
                .save(**kargs)

        elif pmd['service'] == 'mysql':
            driver = "com.mysql.cj.jdbc.Driver"
            obj.write\
                .format('jdbc')\
                .option('url', url)\
                .option("dbtable", rmd['path'])\
                .option("driver", driver)\
                .option("user",pmd['username'])\
                .option('password',pmd['password'])\
                .save(**kargs)

        elif pmd['service'] == 'postgres':
            driver = "org.postgresql.Driver"
            obj.write \
                .format('jdbc') \
                .option('url', url) \
                .option("dbtable", rmd['path']) \
                .option("driver", driver) \
                .option("user", pmd['username']) \
                .option('password', pmd['password']) \
                .save(**kargs)
        elif pmd['service'] == 'elastic':
            uri = 'http://{}:{}'.format(pmd["hostname"], pmd["port"])

            # print(options)
            if "mode" in kargs and kargs.get("mode") == "overwrite":
                mode = "overwrite"
            else:
                mode = "append"
            elatic_write(obj, uri, mode, rmd["path"], options["settings"], options["mappings"])
        elif pmd['service'] == 'oracle':
            url = "jdbc:oracle:thin:{}/{}@//{}:{}/{}".format(pmd['username'], pmd['password'], pmd['hostname'],
                                                             pmd.get('port', '1521'), pmd['database'])
            print(url)
            driver = "oracle.jdbc.driver.OracleDriver"
            return obj.write.format('jdbc').option('url', url) \
                .option("dbtable", md['path']).option("driver", driver) \
                .save(**kargs)
        else:
            raise('downt know how to handle this')

        logger.info({'format':format,'service':pmd['service'], 'path':rmd['path'], 'url':md['url']}, extra={'dlf_type':'engine.write'})

    def elastic_read(self, url, query):
        results = elastic_read(url, query)
        rows = [pyspark.sql.Row(**r) for r in results]
        return self.context().createDataFrame(rows)

    def ingest(self, src_resource=None, src_path=None, src_provider=None, 
                     dest_resource=None, dest_path=None, dest_provider=None, ingest_options=None):
        #0. Process parameteers
        md_src = data.metadata(src_resource, src_path, src_provider)
        if not md_src:
            print('no valid source resource found')
            return

        pd_src = md_src['provider']
        rs_src =  md_src['resource']
        options = utils.merge(pd_src.get('ingest',{}), rs_src.get('ingest',{}))
        options = utils.merge(options, ingest_options)

        if (not dest_resource) and (not dest_path) and dest_provider:
            dest_path = rs_src['path']

        md_dest = data.metadata(dest_resource, dest_path, dest_provider)
        if not md_dest:
            print('no valid destination resource found')
            return

        #1. Get ingress policy
        ingest_policy = options.get('policy', 'hash')

        #Get ingest date 
        yesterday = date.today() - timedelta(1)
        end_date = options.get('date', yesterday.strftime('%Y-%m-%d'))
        ingest_date = datetime.strptime(end_date, '%Y-%m-%d').date()

        #Get ingest key column 
        column = options.get('column', None)

        #Get ingest window
        window =  options.get('window', None)

        #build condition
        condition_sql = ''

        if ingest_policy == 'date':
            condition_sql = '{} <= "{}" '.format(column, end_date)
            if window:
                condition_sql += 'and {} <= "{}" '.format(column, (ingest_date + timedelta(window)).strftime('%Y-%m-%d'))

        print('condition: {}'.format(condition_sql))

        ## 2. Get last schema 
        schema_path = '{}/schema'.format(md_dest['resource']['path'])

        schema_date = ingest_date
        schema_existed = True
        try:
            df_schema = self.read(path=schema_path,provider=dest_provider)
            schema_date = df_schema.sort(desc("date")).limit(1).collect()[0]['date']
        except Exception as ex: 
            logger.error("Not find existed schema file")
            schema_existed = False

        ## 3. Read all resources and check difference
        df_src = self.read(path = md_src['resource']['path'], provider = md_src['resource']['provider'])   

        df_path = '{}/{}'.format(md_dest['resource']['path'], schema_date.strftime('%Y-%m-%d'))  ## Pretend first time ingest

        if condition_sql: #date policy
            diff_df = df_src.where(condition_sql)

        schema_changed = False
        try:
            df_dest = self.read(path=df_path, provider=md_dest['resource']['provider'])
            df_dest = df_dest.drop('ingest_date') if 'ingest_date' in df_dest.columns else df_dest

            if condition_sql: #date policy 
                #Apply condition to both source and destination
                df_dest = df_dest.where(condition_sql) 

            #Check difference in schemas
            if df_src.schema.json == df_dest.schema.json:
                diff_df = diff_df.subtract(df_dest)
            else:
                schema_changed = True
        except Exception as ex: 
            logger.error("Error read destination.")

        if schema_changed or not schema_existed:
            #Different schema, update new
            new_df_schema = self._ctx.createDataFrame([(ingest_date,)], ['date'])
            self.write(new_df_schema, path=schema_path, provider=md_dest['resource']['provider'], mode='append')
            #Update new data object name
            df_path = '{}/{}'.format(dest_path, end_date)

        ### 4. Store dataframe 
        if diff_df.count():
            diff_df = diff_df.withColumn('ingest_date', lit(end_date))
            self.write(diff_df, path=df_path, provider=md_dest['resource']['provider'], mode='append', partitionBy='ingest_date')

def elastic_read(url, query):
    """
    :param format: spark|python (removed pandas)
    :param uri:
    :param action:
    :param query:
    :param kargs:
    :return:

    sample resource:
    - variables are enclosed in [[ ]] instead of {{ }}
    keywords_search:
        provider: elastic_test
        #index: search_keywords_dev
        path: /search_keywords_dev/keyword/
        search: _search
        query: >
            {
              "size" : 2,
              "from" : 0,
              "query": {
                "function_score": {
                  "query": {
                      "match" : {
                        "query_wo_tones": {"query":"[[query]]", "operator" :"or", "fuzziness":"AUTO"}
                    }
                  },
                  "script_score" : {
                      "script" : {
                        "source": "Math.sqrt(doc['impressions'].value)"
                      }
                  },
                  "boost_mode":"multiply"
                }
              }
            }
    :param resource:
    :param path:
    :param provider:
    :param kargs: params to replace in `query` template
    :return: pandas dataframe
    """

    try:
        es = elasticsearch.Elasticsearch([url])
        res = es.search(body=query)
    except:
        raise ValueError("Cannot query elastic search")

    hits = res.pop("hits", None)
    if hits is None:
        #handle ERROR
        raise ValueError("Query failed")

        # res["total_hits"] = hits["total"]
        # res["max_score"] = hits["max_score"]
        # print("Summary:", res)

    hits2 = []
    for hit in hits['hits']:
        hitresult = hit.pop("_source", None)
        hits2.append({**hit, **hitresult})

    return hits2


def elatic_write(obj, uri, mode='append', indexName=None, settings=None, mappings=None):
    """
    :param mode: overwrite | append
    :param obj: spark dataframe, pandas dataframe, or list of Python dictionaries
    :param kargs:
    :return:
    """
    es = elasticsearch.Elasticsearch([uri])
    if mode == "overwrite":
        if isinstance(mappings["properties"], str): # original properties JSON in Elastics format
            # properties: >
            #                 {
            #                     "count": {
            #                         "type": "integer"
            #                     },
            #                     "keyword": {
            #                         "type": "keyword"
            #                     },
            #                     "query": {
            #                         "type": "text",
            #                         "fields": {
            #                             "keyword": {
            #                                 "type": "keyword",
            #                                 "ignore_above": 256
            #                             }
            #                         }
            #                     }
            #                 }
            mappings["properties"] = json.loads(mappings["properties"])
        else: # dictionary
            #             properties:
            #                 count: integer
            #                 keyword: keyword
            #                 query:
            #                     type: text
            #                     fields:
            #                         keyword:
            #                             type: keyword
            #                             ignore_above: 256
            #                 query_wo_tones:
            #                     type: text
            #                     fields:
            #                         keyword:
            #                             type: keyword
            #                             ignore_above: 256
            for k, v in mappings["properties"].items():
                if isinstance(mappings["properties"][k], str):
                    mappings["properties"][k] = {"type":mappings["properties"][k]}

        if isinstance(settings, str):  # original settings JSON in Elastics format
            #       settings: >
            #                 {
            #                     "index": {
            #                         "number_of_shards": 1,
            #                         "number_of_replicas": 3,
            #                         "mapping": {
            #                             "total_fields": {
            #                                 "limit": "1024"
            #                             }
            #                         }
            #                     }
            #                 }
            settings = json.loads(settings)
        else: # yaml object parsed into python dictionary
            #         settings:
            #             index:
            #                 number_of_shards: 1
            #                 number_of_replicas: 3
            #                 mapping:
            #                     total_fields:
            #                         limit: 1024
            pass

        print(settings)
        print(mappings["properties"])

        if not settings or not settings:
            raise ("'settings' and 'mappings' are required for 'overwrite' mode!")
        es.indices.delete(index=indexName, ignore=404)
        es.indices.create(index=indexName, body={
            "settings": settings,
            "mappings": {
                mappings["doc_type"]: {
                    "properties": mappings["properties"]
                }
            }
        })
    else: # append
        pass

    # import pandas.core.frame
    # import pyspark.sql.dataframe
    # import numpy as np
    # if isinstance(obj, pandas.core.frame.DataFrame):
    #     obj = obj.replace({np.nan:None}).to_dict(orient='records')
    # elif isinstance(obj, pyspark.sql.dataframe.DataFrame):
    #     obj = obj.toPandas().replace({np.nan:None}).to_dict(orient='records')
    # else: # python list of python dictionaries
    #     pass

    if isinstance(obj, pyspark.sql.dataframe.DataFrame):
        obj = [row.asDict() for row in obj.collect()]
    else: # python list of python dictionaries
        pass

    from collections import deque
    deque(elasticsearch.helpers.parallel_bulk(es, obj, index=indexName,
                                              doc_type=mappings["doc_type"]), maxlen=0)
    es.indices.refresh()


def get(name):
    global engines

    #get
    engine = engines.get(name)

    if not engine:
        #create
        md = params.metadata()
        cn = md['engines'].get(name)
        config = cn.get('config', {})

        if cn['context']=='spark':
            engine = SparkEngine(name, config)
            engines[name] = engine

    return engine
