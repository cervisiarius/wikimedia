from sklearn import cross_validation, grid_search
from sklearn.cross_validation import StratifiedKFold
import multiprocessing
from sklearn.linear_model import LinearRegression, SGDClassifier, LogisticRegression

from scipy.sparse import coo_matrix, hstack
import sklearn
from scipy.stats import spearmanr
from sklearn.metrics import f1_score, roc_auc_score,accuracy_score, make_scorer
from sklearn.metrics import r2_score, mean_squared_error
from sklearn.preprocessing import StandardScaler

from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier


def rmse(x, y):
    return mean_squared_error(x,y)**0.5


def get_X(M, df, index_col, feature_cols):
    if M is None or index_col is None:
        l = []
    else:
        l = [M[df[index_col]], ]
    for col in feature_cols:
         l.append(coo_matrix(df[col].astype('float')).T)
    return hstack(l).toarray()


def f1(x, y):
    sr = f1_score(x, y)
    print 'F1: %2.5f' % sr
    return sr

def auc(y_true,y_pred):
    roc = roc_auc_score(y_true, y_pred)
    print 'ROC: %2.5f \n' % roc
    return roc

def acc(y_true,y_pred):    
     r2 = accuracy_score(y_true, y_pred)
     print 'Accuracy: %2.5f' % r2
     return r2

def multi_score_classification(y_true,y_pred):   
    f1(y_true,y_pred) #set score here and not below if using MSE in GridCV
    acc(y_true,y_pred)
    score = auc(y_true,y_pred)
    return score

def multi_scorer_classification():
    return make_scorer(multi_score_classification, greater_is_better=True) # change for false if using MSE


def spearman(x, y):
    sr = spearmanr(x, y)[0]
    print 'Spearman: %2.5f \n' % sr
    return sr

def RMSE(y_true,y_pred):
    rmse = rmse(y_true, y_pred)
    print 'RMSE: %2.5f' % rmse
    return rmse

def R2(y_true,y_pred):    
     r2 = r2_score(y_true, y_pred)
     print 'R2: %2.5f' % r2
     return r2

def multi_score_regression(y_true,y_pred):    
    RMSE(y_true,y_pred) #set score here and not below if using MSE in GridCV
    R2(y_true,y_pred)
    score = spearman(y_true,y_pred)
    return score

def multi_scorer_regression():
    return make_scorer(multi_score_regression, greater_is_better=True) # change for false if using MSE

  
def cv (X, y, folds, alg, param_grid, regression):
    """
    Determine the best model via cross validation. This should be run on training data.
    """
    if regression:
        scoring = multi_scorer_regression()
    else:
        scoring = multi_scorer_classification()
        
    print "\n\n\nDoing Gridsearch\n"

    kfold_cv = cross_validation.KFold(X.shape[0], n_folds=folds, shuffle=True)
    model = grid_search.GridSearchCV(cv  = kfold_cv, estimator = alg, param_grid = param_grid, scoring = scoring) #n_jobs=multiprocessing.cpu_count()
    model = model.fit(X,y)
    # model trained on all data
    y_pred = model.predict(X)
    
    if regression:
        print "Best Model Train RMSE: %f" % rmse(y, y_pred)
        print "Best Model Train Spearman %f" % spearman(y, y_pred)
    else:
        print "Best Model Train AUC: %f" % roc_auc_score(y, y_pred)
        print "Best Model Train F1 %f" % f1_score(y, y_pred)
        print "Best Model Train Accuracy %f" % accuracy_score(y, y_pred)
        


    print("\nBest parameters set found:")
    best_parameters, score, _ = max(model.grid_scores_, key=lambda x: x[1])
    print(best_parameters, score)
    print "\n"
    print("Grid scores:")
    for params, mean_score, scores in model.grid_scores_:
        print("%0.5f (+/-%0.05f) for %r"
              % (mean_score, scores.std() / 2, params))

    return model



import copy
from sklearn.cross_validation import train_test_split

def get_scores(model, X):
    try:
        scores = model.decision_function(X)
    except:
        scores = model.predict_proba(X)[:, 1]
    return scores

def evaluate(X, y, alg, regression, decision_metric, retrain_model = False, verbose = False ):
    
    
    X_train, X_test, y_train, y_test = train_test_split( X, y, test_size=0.33, random_state=42)
    model = alg.fit(X_train,y_train)
    y_train_pred = model.predict(X_train)
    y_test_pred  = model.predict(X_test)
    
    if not regression:
        y_train_proba = get_scores(model,X_train)
        y_test_proba = get_scores(model,X_test)
        
        
    
    if regression and verbose:
        print "Train RMSE: %f" % rmse(y_train, y_train_pred)
        print "Test RMSE: %f" % rmse(y_test, y_test_pred)
        
        print "\nTrain Spearman %f" % spearmanr(y_train, y_train_pred)[0]
        print "Test Spearman %f" % spearmanr(y_test, y_test_pred)[0]

    elif not regression and verbose:
        
        print "Train AUC: %f" % roc_auc_score(y_train, y_train_proba)
        print "Test AUC: %f" % roc_auc_score(y_test, y_test_proba)

        print "\nTrain F1 %f" % f1_score(y_train, y_train_pred)
        print "Test F1 %f" % f1_score(y_test, y_test_pred)

        print "\nTrain Accuracy %f" % accuracy_score(y_train, y_train_pred)
        print "Test Accuracy %f" % accuracy_score(y_test, y_test_pred)
        
    
    if retrain_model:
        model =  alg.fit(X,y)
        
    if decision_metric == 'AUC':
        test_metric = roc_auc_score(y_test, y_test_proba)
        train_metric = roc_auc_score(y_train, y_train_pred)
    elif decision_metric == 'F1':
        test_metric = f1_score(y_test, y_test_pred)
        train_metric = f1_score(y_test, y_test_pred)
    elif decision_metric == 'Accuracy':
        test_metric = accuracy_score(y_test, y_test_pred)
        train_metric = accuracy_score(y_train, y_train_pred)
    elif decision_metric == 'RMSE':
        test_metric = - rmse(y_test, y_test_pred)
        train_metric = - rmse(y_train, y_train_pred)
    elif decision_metric == 'Spearman':
        test_metric = spearmanr(y_test, y_test_pred)[0]
        train_metric = spearmanr(y_train, y_train_pred)[0]
         
    return model, train_metric, test_metric


def seq_forw_select(features, max_k, importance_model, print_steps=True):
    features = copy.deepcopy(features)
    # Initialization
    feat_sub = []
    feat_sub_set_names = []
    k = 0
    d = len(features)
    if max_k > d:
        max_k = d
  
    while True:
        print ('\n\n')
        available_features = features.keys()
        # Inclusion step
        if print_steps:
            print('Available Features', available_features)
        _, train_metric, crit_func_max = importance_model.build(feat_sub + features[available_features[0]])
        if print_steps:
                print (feat_sub_set_names + [available_features[0]], ': ', (train_metric, crit_func_max))
                
        best_feat = available_features[0]
        for x in available_features[1:]:
            _, train_metric, crit_func_eval = importance_model.build(feat_sub + features[x])
            if print_steps:
                print (feat_sub_set_names + [x], ': ', (train_metric, crit_func_eval))
            if crit_func_eval > crit_func_max:
                crit_func_max = crit_func_eval
                best_feat = x
        feat_sub += features[best_feat]
        feat_sub_set_names.append(best_feat)
        del features[best_feat]
        
        if print_steps:
            print('Winning Subset: ', feat_sub_set_names, ': ', crit_func_max)
        

        # Termination condition
        k = len(feat_sub_set_names)
        if k == max_k:
            break

class ImportanceRegressor:
    
    def __init__(self, df, y_name, decision_metric, alg ):
        self.y = df[y_name].astype('float').values
        self.df = df
        self.decision_metric = decision_metric
        self.alg = alg

         
    def build(self, features, retrain_model = False):
        if 'indx' in features:
            X = get_X(M, self.df, 'indx', features)
        else:
            X = get_X(None, self.df, 'indx', features)
        scaler = StandardScaler().fit(X)
        X = scaler.transform(X) 
    
        return evaluate(X, self.y, self.alg, True, self.decision_metric, retrain_model = retrain_model)
        
        
    
class ImportanceClassifier:
    def __init__(self, df, y_name, decision_metric, alg ):
        self.y = df['in_t'].astype('int').values
        self.df = df
        self.decision_metric = decision_metric
        self.alg = alg
         
            
    def build(self, features, retrain_model = False):
        if 'indx' in features:
            X = get_X(M, self.df, 'indx', features)
        else:
            X = get_X(None, self.df, 'indx', features)
            
        scaler = StandardScaler().fit(X)
        X = scaler.transform(X) 
        
        return evaluate(X, self.y, self.alg, False, self.decision_metric, retrain_model = retrain_model)