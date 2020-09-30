import pandas as pd
import os
from pandas_profiling import ProfileReport

from sklearn.pipeline import Pipeline
from sklearn import model_selection
from sklearn import metrics

import scikitplot as skplt
import matplotlib.pyplot as plt

import lightgbm as lgb

ML_TRAIN_DIR = os.path.join( os.path.abspath('.'), *"crm_churn/src/ml/train".split("/") )
ML_TRAIN_DIR = os.path.dirname( os.path.abspath(__file__) )
ML_DIR = os.path.dirname(ML_TRAIN_DIR)
SRC_DIR = os.path.dirname(ML_DIR)
BASE_DIR = os.path.dirname(SRC_DIR)
DATA_TRAIN_DIR = os.path.join( BASE_DIR, 'data', 'train' )
MODELS_DIR = os.path.join( BASE_DIR, 'models' )

# Importando a base de dados
df = pd.read_csv( os.path.join(DATA_TRAIN_DIR, 'abt_churn.csv'), sep="|" )

# Definindo e removendo colunas para o modelo
all_columns = df.columns.tolist()
target = 'flag_churn'
to_remove = [i for i in df.columns if 'partition' in i ] + ['seller_id', 'dt_ref'] + [target]
all_columns = list(set( all_columns ) - set(to_remove))

# Checando variáveis numerias e categóricas
df[all_columns].dtypes[ df[all_columns].dtypes == 'object']

df[all_columns].describe().T

profile = ProfileReport(df[all_columns + [target]].copy(), title="Pandas Profiling Report")
profile.to_file("your_report.html", minimal=True)

X_train, X_test, y_train, y_test = model_selection.train_test_split(df[all_columns],
                                                                    df[target],
                                                                    test_size=0.15,
                                                                    random_state=42)

model = lgb.LGBMClassifier(n_jobs=-1, random_state=42, metric='auc' )

params = {"num_leaves":[20,50],
          "max_depth":[8,10,12,15],
          "n_estimators":[100,250,500],
          "learning_rate":[0.01,0.1,0.9],
          "subsample":[0.1,0.20,0.5,0.7,1] }

search = model_selection.RandomizedSearchCV(model, params, cv=3, scoring='roc_auc', verbose=5000, n_iter=100)
search.fit(X_train, y_train)
df_search = pd.DataFrame( search.cv_results_)

best_pars = df_search['params'][ df_search.rank_test_score == 1 ].iloc[0]

model = lgb.LGBMClassifier(n_jobs=-1,
                            random_state=42,
                            metric='auc',
                            **best_pars )

model.fit(X_train, y_train) # Foi treinado esse carai!!

X_train.columns.tolist()

y_pred_train = model.predict_proba(X_train)
auc_train = metrics.roc_auc_score(y_train, y_pred_train[:,1])
print("Curva roc com base de treino:", auc_train)

skplt.metrics.plot_roc(y_train, y_pred_train)
plt.show()

y_pred_test = model.predict_proba(X_test)
auc_test = metrics.roc_auc_score(y_test, y_pred_test[:,1])
print("Curva roc com base de test:", auc_test)

skplt.metrics.plot_roc(y_test, y_pred_test)
plt.show()

skplt.metrics.plot_lift_curve(y_test, y_pred_test)
plt.show()

skplt.metrics.plot_ks_statistic(y_test, y_pred_test)
plt.show()

y_test_pred_new = y_pred_test[:,1] > 0.3

metrics.precision_score( y_test, y_test_pred_new )


model_pkl = pd.Series( {"model": model,
                        "fit_vars": X_train.columns.tolist(),
                        "auc": {"test":auc_test,
                                "train":auc_train}
                        } )

model_pkl.to_pickle( os.path.join(MODELS_DIR,'model_churn.pkl') )