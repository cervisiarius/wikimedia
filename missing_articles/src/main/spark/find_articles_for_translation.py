from pyspark import SparkConf, SparkContext
import argparse
import os
import codecs
import networkx as nx
from collections import Counter
from pprint import pprint


"""
Usage: 

/home/otto/spark-1.3.0-bin-hadoop2.4/bin/spark-submit \
--driver-memory 5g --master yarn --deploy-mode client \
--num-executors 2 --executor-memory 10g --executor-cores 8 \
--queue priority \
/home/ellery/wikimedia/missing_articles/src/main/spark/find_articles_for_translation.py --dir
"""

home_dir = '/home/ellery/'
hadoop_home_dir = '/user/ellery/'

def get_parser(names):
    def loadRecord(line):
        cells = line.strip().split('\t')
        return dict(zip(names, cells))
    return loadRecord


def create_graph(sc, s, t, delim, wd_languages, rd_languages, ill_languages_from, ill_languages_to):
    G = nx.Graph()

    # add wikidata lik=nks
    names = ["id", "language_code", "article_name"]
    wikidata_links = sc.textFile("/user/west1/interlanguage_links.tsv").map(get_parser(names))\
                    .filter(lambda x: x['language_code'] in wd_languages and x['id'].startswith('Q'))\
                    .filter(lambda x: ':' not in x['article_name'])\
                    .filter(lambda x: not x['article_name'].startswith('List'))\
                    .map(lambda x: ('wikidata'+ delim + x['id'], x['language_code'] + delim + x['article_name']))         
    G.add_edges_from(wikidata_links.collect())
    print "Got Wikidata Links"

    # add interlanguage links
    names = ['ll_from', 'll_to', 'll_lang']
    for ill_lang in ill_languages_from:
        ill = sc.textFile('/user/hive/warehouse/prod_tables.db/' + ill_lang + 'wiki_langlinks_joined')\
        .map(lambda x: x.split('\t'))\
        .filter(lambda x: x[2] in ill_languages_to and len(x[1]) > 0 and len(x[0]) > 0)\
        .map(lambda x: (ill_lang + delim + x[0], x[2] + delim + x[1]))
        G.add_edges_from(ill.collect())
        print "Got ILL links for %s" % ill_lang

    # add redirect links
    names = ['rd_from', 'rd_to']
    for rd_lang in rd_languages:
        rd = sc.textFile("/user/hive/warehouse/prod_tables.db/" + rd_lang + "wiki_redirect_joined")\
        .map(lambda x: x.split('\t'))\
        .map(lambda x: (rd_lang + delim + x[0], rd_lang + delim + x[1]))
        G.add_edges_from(rd.collect())
        print "got rdd links for %s" % rd_lang

    return G



def test_clustering(G, n, s, t, delim):
    """
    Given a node, find the connected component n is in.
    """
    sub_g = G.subgraph(nx.node_connected_component(G,n ))
    print ("Nodes:")
    pprint(sub_g.nodes())
    print("\nEdges")
    pprint(sub_g.edges())
    print("\nCluster")
    pprint(get_merged_cluster(tumor_g, s, t, delim))
    

def is_subgraph_missing_target_item(g, s, t, delim):
    """
    Given a subgraph, see of it is missing an article in
    the target language
    """
    has_s = False
    has_t = False
    missing_items = {}
    
    for e in g.edges_iter():
        e_dict = {}
        l1, n1 = e[0].split(delim)
        l2, n2 = e[1].split(delim)
        e_dict[l1] = n1
        e_dict[l2] = n2
        
        if 'wikidata' in e_dict and s in e_dict:
            has_s = True
            missing_items[e[0].split(delim)[1]] = e[1].split(delim)[1] 
            
        if 'wikidata' in e_dict and t in e_dict:
            has_t = True
                
    if has_s and not has_t:
        return missing_items
    else:
        return {}
    

def get_missing_items(sc, G, s, t, delim, exp_dir, n = 100):
    """
    Find all items in s missing in t
    """
    cc = nx.connected_component_subgraphs (G)
    missing_items = {}
    for i, g in enumerate(cc):
        missing_items.update(is_subgraph_missing_target_item(g, s, t, delim))
        
    missing_items = sc.parallelize(missing_items.items())

    #names = ['wikidata_id', 'lang', 'article_title', 'pageview_count']
    pageviews = sc.textFile('/user/west1/pagecounts/wikidata_only')\
    .map(lambda x: x.split('\t'))\
    .filter(lambda x: x[1] == s)\
    .map(lambda x: (x[0], int(x[3])))
    missing_items = missing_items.join(pageviews)

    ranked_missing_items = missing_items.sortBy(lambda x: -x[1][1])

    def tuple_to_str(t):
        item_id, (item_name, n) = t
        line = item_id + '\t' + item_name + '\t' + str(n) + '\n'
        return line



    zip_dir = 'missing_articles_dir'
    zip_file = 'missing_articles.tsv.gz'
    base_dir = os.path.join(home_dir, exp_dir)
    hadoop_base_dir = os.path.join(hadoop_home_dir, exp_dir)

    local_zip_dir = os.path.join(base_dir, zip_dir)
    local_zip_file = os.path.join(base_dir, zip_file)
    hdfs_zip_dir = os.path.join(hadoop_base_dir, zip_dir)

    if not os.path.exists(base_dir):
        os.makedirs(base_dir)
    print os.system( 'hadoop fs -mkdir ' + hadoop_base_dir)
    print os.system('hadoop fs -rm -r ' + hdfs_zip_dir)

    ranked_missing_items.map(tuple_to_str).saveAsTextFile ("hdfs://" + hdfs_zip_dir , compressionCodecClass  =  "org.apache.hadoop.io.compress.GzipCodec")
    print os.system('hadoop fs -copyToLocal ' + hdfs_zip_dir + ' ' + local_zip_dir)
    print os.system( 'cat ' + local_zip_dir + '/*.gz > ' + local_zip_file)
    print os.system( 'gunzip ' + local_zip_file)
    print os.system('rm -r ' + local_zip_dir)
    

    


def get_cluster(g, s, t, delim):
    """
    """
    merged_items = set()
    has_s = False
    has_t = False
    wikidata_items = set()
    
    for e in g.edges_iter():
        e_dict = {}
        l1, n1 = e[0].split(delim)
        l2, n2 = e[1].split(delim)
        e_dict[l1] = n1
        e_dict[l2] = n2
        
        if 'wikidata' in e_dict and s in e_dict:
            has_s = True
            wikidata_items.add(e_dict['wikidata'])
            merged_items.add(e)
            
        if 'wikidata' in e_dict and t in e_dict:
            has_t = True
            wikidata_items.add(e_dict['wikidata'])
            merged_items.add(e)
        
    if len(wikidata_items) > 1 and has_s and has_t:
        return merged_items, wikidata_items
    else:
        return None, None


def get_clusters(G, s, t, delim, path):
    gs = nx.connected_component_subgraphs (G)
    clusters = []
    for g in gs:
        cluster, items = get_cluster(g, s, t)
        if cluster:
            clusters.append(cluster)

    with open(path + '_clusters', 'w') as f:
        for cluster in clusters:
            f.write( "\n")
            for edge in cluster:
                line = edge[0] + " " + edge[1] + '\n'
                f.write(line.encode('utf8'))
     


if __name__ == '__main__':


    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--dir', required = True, help='experiment dir' )
    args = parser.parse_args()
    exp_dir = args.dir

    delim = '|PIPE|'
    s = 'en'
    t = 'es'
    wd_languages = set(['en', 'de', 'es'])
    rd_languages = set(['en', 'de', 'es', 'wikidata'])
    ill_languages_from = set(['en', 'de', 'es'])
    ill_languages_to = set(['en', 'de', 'es'])

    conf = SparkConf()
    conf.set("spark.app.name", 'als')
    conf.set("spark.core.connection.ack.wait.timeout", "180")
    conf.set("spark.akka.frameSize", '2047')
    sc = SparkContext(conf=conf)


    G = create_graph(sc, s, t, delim, wd_languages, rd_languages, ill_languages_from, ill_languages_to)
    print "Got entire Graph"
    get_missing_items(sc, G, s, t, delim, exp_dir, n = 10000)
    print "Got missing Items"
    path = os.path.join(home_dir, exp_dir)
    get_clusters(G, s, t, delim, path)
    print "Got clusters"
    
    

    



    