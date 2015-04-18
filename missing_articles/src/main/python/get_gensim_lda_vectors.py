import string
import gensim
import os
import time
import argparse






if __name__ == '__main__':


    parser = argparse.ArgumentParser()
    parser.add_argument('--dir', required = True, help='experiment data directory' )
    args = parser.parse_args()

    home_dir = '/home/ellery'
    hadoop_home_dir = '/user/ellery'
    exp_dir = args.dir 
    base_dir = os.path.join(home_dir, exp_dir)
    hadoop_base_dir = os.path.join(hadoop_home_dir,  exp_dir)
    dict_file = 'dictionary.txt'
    article_name_file = 'articles.txt'
    article_file = 'articles.blei'
    article_vectors_file = 'article_vectors.txt'

    dictionary = gensim.corpora.dictionary.Dictionary() 
    id2Token = dict(enumerate(l[:-1] for l in open(os.path.join(base_dir, dict_file)))) 
    dictionary.token2id  = {v: k for k, v in id2Token.items()}
    corpus = gensim.corpora.bleicorpus.BleiCorpus(os.path.join(base_dir, article_file), fname_vocab= os.path.join(base_dir, dict_file)) 


    time1 = time.time()
    model = gensim.models.ldamulticore.LdaMulticore(corpus=corpus,\
                                num_topics=400,\
                                id2word=dictionary,\
                                workers=4,\
                                chunksize=10000,\
                                passes=1,\
                                batch=False,\
                                alpha='symmetric',\
                                eta=None,\
                                decay=0.5,\
                                offset=1.0,\
                                eval_every=10,\
                                iterations=50,\
                                gamma_threshold=0.001)
    time2 = time.time()
    print 'training lda model took %0.3f minutes' % ((time2-time1) / 60.0)
    model.save(os.path.join(base_dir, 'lda_model'))

    def write_doc_vectors_to_file(model, corpus):
        # write reduced document vectors to file
        article_name_f = open(os.path.join(base_dir, article_name_file), 'r')
        article_f = open(os.path.join(base_dir, article_file), 'r')
        article_vectors_f = open(os.path.join(base_dir, article_vectors_file), 'w')

        for i in range(len(corpus)):
            doc = [ p.split(':') for p in next(article_f)[:-1].split(' ')[1:]]
            doc = [(int(p[0]), int(p[1])) for p in doc]
            line = next(article_name_f).strip() + ' '
            v = model[doc]
            str_v = [ str(topic_id)+':'+str(weight) for topic_id, weight in v ]
            line += ' '.join(str_v) + '\n'
            article_vectors_f.write(line)
        if i % 10000 == 0:
            print line

    
    time1 = time.time()
    write_doc_vectors_to_file(model, corpus)
    time2 = time.time()
    print 'creating lda vectors took %0.3f minutes' % ((time2-time1) / 60.0)

    # move document vectors to hdfs

    print os.system('hadoop fs -mkdir ' + hadoop_base_dir )
    print os.system('hadoop fs -put ' + os.path.join(base_dir, article_vectors_file ) + ' ' + os.path.join(hadoop_base_dir, article_vectors_file))

