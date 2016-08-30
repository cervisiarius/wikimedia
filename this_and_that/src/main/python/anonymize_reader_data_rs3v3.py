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
    --num-executors 20 \
    --executor-memory 5g \
    --executor-cores 4 \
    --queue priority \
/home/west1/wikimedia/trunk/this_and_that/src/main/python/anonymize_reader_data_rs3v3.py \
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
    row = line.strip().split('\t')
    if len(row) !=5:
        return None
    
    d = {
         'ip': row[0],
         'user_agent': row[1],
         'geocoded_data' : parse_hive_struct(row[2]),
         'user_agent_map' : parse_hive_struct(row[3]),
         'requests' : parse_requests(row[4])
        }
    return d


def parse_requests(requests):
    ret = []
    for r in requests.split('REQUEST_DELIM'):
        t = r.split('|')
        if (len(t) % 2) != 0: # should be list of (name, value) pairs and contain at least id,ts,title
            continue
        data_dict = { t[i]: t[i+1] for i in range(0, len(t), 2) }
        ret.append(data_dict)
    ret.sort(key = lambda x: x['ts']) # sort by time
    return ret

    
if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--key', required=True, 
        help='hash key'
    )

    args = parser.parse_args()

    input_dir = '/user/hive/warehouse/traces.db/rs3v3'
    output_dir = '/user/west1/reader_research/anonymized_traces/rs3v3'
    key = args.key.strip()
    
    conf = SparkConf()
    conf.set("spark.app.name", 'Anonymize rs3v3')
    sc = SparkContext(conf=conf, pyFiles=[])

    def modify_fields(x):
        # Hash ip.
        x['ip'] = hmac.new(key.encode('utf-8'), x['ip'].encode('utf-8'), hashlib.sha1).hexdigest()
        # Hash user_agent.
        x['user_agent'] = hmac.new(key.encode('utf-8'), x['user_agent'].encode('utf-8'), hashlib.sha1).hexdigest()
        # Prune geocoded_data; keep only country_code, timezone, continent, country.
        for k in ['latitude', 'longitude', 'postal_code', 'city', 'subdivision']:
          if k in x['geocoded_data']: del x['geocoded_data'][k]
        # Prune user_agent_map; keep only wmf_app_version, device_family, browser_family, os_family.
        for k in ['browser_major', 'os_major', 'os_minor']:
          if k in x['user_agent_map']: del x['user_agent_map'][k]
        return x

    
    trace_rdd = sc.textFile(input_dir) \
        .map(parse_row) \
        .filter(lambda x: x is not None) \
        .map(modify_fields) \
        .map(lambda x: json.dumps(x))         
    os.system('hadoop fs -rm -r ' + output_dir)
    trace_rdd.saveAsTextFile(output_dir)
