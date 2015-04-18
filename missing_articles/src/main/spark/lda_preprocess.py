from operator import add
from pyspark import SparkConf, SparkContext
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer
from pyspark.mllib.regression import LabeledPoint
import string
import gensim
import os
import argparse

"""
Usage: 

/home/otto/spark-1.3.0-bin-hadoop2.4/bin/spark-submit \
--driver-memory 5g --master yarn --deploy-mode client \
--num-executors 4 --executor-memory 20g --executor-cores 8 \
/home/ellery/wikimedia/missing_articles/src/main/spark/lda_preprocess.py --dir exp_dir

To run LDA, see wikimedia/missing_articles/src/main/python/get_gensim_lda.py
"""


def load_articles(files):
    """
    Right now we have files for enwiki and simple wiki where each line 
    corresponds to the full article text. Tokens seperated by spaces
    """
    articles = None
    for lang, f in files.iteritems():
        lang_articles = sc.textFile("/user/west1/wikipedia_plaintexts/" + f).map(lambda line: line.split('\t'))
        lang_articles = lang_articles.filter(lambda x: len(x) ==4 and len(x[3]) > 0 and len(x[2]) == 0)
        lang_articles = lang_articles.map(lambda x: ('|'.join([lang, x[0], x[1]]), x[3].split(' ')))
        
        if articles == None:
            articles = lang_articles
        else:
            articles = articles.union(lang_articles)
    return articles


def get_word_id_mapping(articles, min_occurrences = 3, num_tokens = 100000):
    """
    Create a mapping from words to ids. The mapping with contain the "num_tokens" most frequent tokens
    and no token that appears less often than min_occurrences
    """
    words = articles.flatMap(lambda x: x[1])\
    .map(lambda x: (x, 1))\
    .reduceByKey(add)\
    .filter(lambda x: x[1] >= min_occurrences)\
    .top(num_tokens, key=lambda x: x[1])
    
    local_word_map = []
    for w in words:
        t = (w[0], len(local_word_map))
        local_word_map.append(t)
    return dict(local_word_map), sc.parallelize(local_word_map)

def get_banned_words():
    punctuation = set(string.punctuation)
    nltk_stopwords = set(stopwords.words('english'))
    wikimarkup = set(['*nl*', '</ref>', '<ref>', '--', '``', "''", "'s", 'also', 'refer' , '**'])
    banned_words = punctuation.union(nltk_stopwords).union(wikimarkup)
    return banned_words

def clean_article_text(articles, banned_words):
    """

    """
    lower_articles = articles.map(lambda x: (x[0], [w.lower() for w in x[1]]))
    # remove banned words
    clean_articles = lower_articles.map(lambda x: (x[0], [w for w in x[1] if w not in banned_words]))
    # Stem
    def stem_word_list(words):
        stemmer = PorterStemmer()
        return [stemmer.stem(w.lower()) for w in words]
    
    #stemmed_articles = clean_articles.map(lambda x: (x[0], stem_word_list(x[1])))
    return clean_articles #stemmed_articles


def get_term_frequencies(articles):
    """
    Maps each article to a list of (word, freq pairs). Each article now has the from
    (article_id, list((word, freq pairs)))
    """
    def map_tokens_to_frequencies(x):
        doc_id, tokens = x
        from collections import Counter
        frequencies = list(Counter(tokens).items())
        return (doc_id, frequencies)  
    return articles.map(map_tokens_to_frequencies)


def translate_words_to_ids(tf_articles, word_id_map):
    """
    Map (article_id, list((word, freq pairs))) to (article_id, list((word_id, freq pairs)))
    """
    def flatten(x):
        article_id, counts = x
        elems = [(c[0], (article_id, c[1])) for c in counts]
        return elems

    def map_tokens_to_ids(x):
        (token, ((doc_id, count), token_id)) = x
        return (doc_id, (token_id, count))

    return tf_articles.flatMap(flatten).\
    join(word_id_map).\
    map(map_tokens_to_ids).\
    groupByKey()


if __name__ == '__main__':

    files = { 'simple': 'simplewiki-20150406'} #, 'en': 'enwiki-20150304'}
    min_occurrences = 3
    num_tokens = 50000


    parser = argparse.ArgumentParser()
    parser.add_argument('--dir', required = True, help='experiment directory' )
    args = parser.parse_args()

    # set up environment
    conf = SparkConf()
    conf.set("spark.app.name", 'lda_preprocess')
    sc = SparkContext(conf=conf)

    
    # load articles
    tokenized_articles = load_articles(files)
    # normalize
    
    normalized_tokenized_articles = clean_article_text(tokenized_articles, get_banned_words())
    # get word-id maping
    local_word_id_map, word_id_map = get_word_id_mapping(normalized_tokenized_articles, min_occurrences = min_occurrences, num_tokens = num_tokens)
    local_id_word_map = inv_map = {v: k for k, v in local_word_id_map.items()}
    # map list of tokens into list of token:count elements
    tf_articles = get_term_frequencies(normalized_tokenized_articles)
    # map list of token:count elements into list of token-id:count
    id_articles = translate_words_to_ids(tf_articles, word_id_map)


    home_dir = '/home/ellery/'
    hadoop_home_dir = '/user/ellery/'
    exp_dir = args.dir + '/'
    base_dir = home_dir + exp_dir
    hadoop_base_dir = hadoop_home_dir + exp_dir

    dict_file = 'dictionary.txt'

    zip_dir = 'articles.pre_blei_dir'
    zip_file = 'articles.pre_blei.gz'
    combined_file = 'articles.pre_blei'

    article_name_file = 'articles.txt'
    article_file = 'articles.blei'
    article_vectors_file = 'article_vectors.txt'



    print os.system( 'rm -r ' + base_dir)
    print os.system( 'mkdir ' + base_dir)
    print os.system( 'hadoop fs -rm -r ' + hadoop_base_dir)
    print os.system( 'hadoop fs -mkdir ' + hadoop_base_dir)



    # save dictionary to file: word at line i has id i
    with open(base_dir + dict_file, 'w') as f:
        for word, word_id in sorted(local_word_id_map.items(), key=lambda x:x[1]):
            line = word + '\n'
            f.write(line.encode('utf8'))

        # save rdd to file
    def tuple_to_str(t):
        article, vector = t
        str_vector = [str(e[0]) + ':' + str(e[1]) for e in vector]
        return article + ' ' + str(len(str_vector)) + ' ' + ' '.join(str_vector)
    id_articles.map(tuple_to_str).saveAsTextFile ("hdfs://" + hadoop_base_dir + zip_dir, compressionCodecClass  =  "org.apache.hadoop.io.compress.GzipCodec")


    print os.system('hadoop fs -copyToLocal ' + hadoop_base_dir + zip_dir + ' ' + base_dir +  zip_dir)
    print os.system( 'cat ' + base_dir + zip_dir + '/*.gz > ' + base_dir + zip_file)
    print os.system( 'gunzip ' + base_dir + zip_file)
    print os.system( "cut  -d' ' -f2- " + base_dir + combined_file + ' > ' + base_dir + article_file)
    print os.system( "cut  -d' ' -f1 " + base_dir + combined_file + ' > ' + base_dir + article_name_file)
