from pyspark import SparkConf, SparkContext
import json
import argparse
import hmac
import hashlib
import os

"""
Usage: 

spark-submit \
    --driver-memory 1g \
    --master yarn \
    --deploy-mode client \
    --num-executors 100 \
    --executor-memory 5g \
    --executor-cores 4 \
    --queue priority \
/home/west1/wikimedia/trunk/this_and_that/src/main/python/anonymize_reader_data_first_week_of_march.py \
    --key 

"""

def parse_hive_struct(s):
    d = {}
    for e in s.split('\x02'):
        if '\x03' in e:
            k,v = e.split('\x03')
            d[k] = v
    return d

def parse_row(line):
    row = line.strip().split('\x01')
    if len(row) != 39:
        return None
    
    d = {
          'hostname':           row[0],
          'sequence':           row[1],
          'dt':                 row[2],
          'time_firstbyte':     row[3], # Delete
          'ip':                 row[4], # Hash
          'cache_status':       row[5], # Delete
          'http_status':        row[6],
          'response_size':      row[7],
          'http_method':        row[8],
          'uri_host':           row[9],
          'uri_path':           row[10],
          'uri_query':          row[11],
          'content_type':       row[12],
          'referer':            row[13],
          'x_forwarded_for':    row[14], # Hash
          'user_agent':         row[15], # Hash
          'accept_language':    row[16],
          'x_analytics':        row[17], # Delete
          'range':              row[18], # Delete
          'is_pageview':        row[19],
          'record_version':     row[20],
          'client_ip':          row[21], # Hash
          'geocoded_data':      parse_hive_struct(row[22]), # Prune
          'x_cache':            row[23], # Delete
          'user_agent_map':     parse_hive_struct(row[24]), # Prune
          'x_analytics_map':    row[25], # Delete
          'ts':                 row[26], # Delete
          'access_method':      row[27],
          'agent_type':         row[28],
          'is_zero':            row[29], # Delete
          'referer_class':      row[30],
          'normalized_host':    row[31], # Delete
          'pageview_info':      row[32], # Delete
          'page_id':            row[33], # Delete
          'webrequest_source':  row[34], # Delete
          'year':               row[35], # Delete
          'month':              row[36], # Delete
          'day':                row[37], # Delete
          'hour':               row[38]  # Delete
        }
    return d

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--key', required=True, help='hash key')
    args = parser.parse_args()

    input_dir = '/user/hive/warehouse/traces.db/first_week_of_march'
    output_dir = '/user/west1/reader_research/anonymized_traces/first_week_of_march'
    key = args.key.strip()
    
    conf = SparkConf()
    conf.set("spark.app.name", 'Anonymize first_week_of_march')
    sc = SparkContext(conf=conf, pyFiles=[])

    def modify_fields(x):
        # Hash ip, user_agent, x_forwarded_for, client_ip.
        for k in ['ip', 'user_agent', 'x_forwarded_for', 'client_ip']:
          x[k] = hmac.new(key.encode('utf-8'), x[k].encode('utf-8'), hashlib.sha1).hexdigest()
        # Prune geocoded_data; keep only country_code, timezone, continent, country.
        for k in ['latitude', 'longitude', 'postal_code', 'city', 'subdivision']:
          if k in x['geocoded_data']: del x['geocoded_data'][k]
        # Prune user_agent_map; keep only wmf_app_version, device_family, browser_family, os_family.
        for k in ['browser_major', 'os_major', 'os_minor']:
          if k in x['user_agent_map']: del x['user_agent_map'][k]
        # Delete some unnecessary fields.
        for k in ['time_firstbyte', 'cache_status', 'x_analytics', 'range', 'x_cache', 'x_analytics_map', \
          'ts', 'is_zero', 'normalized_host', 'pageview_info', 'page_id', 'webrequest_source', \
          'year', 'month', 'day', 'hour']:
          if k in x: del x[k]
        return x

    
    trace_rdd = sc.textFile(input_dir) \
        .map(parse_row) \
        .filter(lambda x: x is not None) \
        .map(modify_fields) \
        .map(lambda x: json.dumps(x))         
    os.system('hadoop fs -rm -r ' + output_dir)
    trace_rdd.saveAsTextFile(output_dir)
