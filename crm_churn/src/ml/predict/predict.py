import pandas as pd
import os
import datetime

ML_PREDICT_DIR = os.path.dirname(os.path.abspath(__file__))
ML_DIR = os.path.dirname(ML_PREDICT_DIR)
SRC_DIR = os.path.dirname(ML_DIR)
BASE_DIR = os.path.dirname(SRC_DIR)
DATA_DIR = os.path.join(BASE_DIR, 'data')
DATA_PREDICT_DIR = os.path.join(DATA_DIR, 'predict')
DATA_SCORE_DIR = os.path.join(DATA_DIR, 'score')
MODEL_DIR = os.path.join(BASE_DIR, 'models')

print("Importando mdoelo...", end="")
model = pd.read_pickle(os.path.join(MODEL_DIR, 'model_churn.pkl'))
print("ok.")

print("Importando a base de dados...", end="")
df = pd.read_csv(os.path.join(DATA_PREDICT_DIR, "tb_predict.csv"),
                 sep="|",
                 usecols= model['fit_vars'] +['dt_ref','seller_id'])
print("Ok.")

print("Escorando...",end="")
tb_score = model['model'].predict_proba( df[model['fit_vars']] )
df_score = df[['seller_id', 'dt_ref']] .copy()
df_score["proba_churn"] = tb_score[:,1]
df_score['dt_atualizacao'] = datetime.datetime.now()
print("Ok.")

print("Salvando a base de escorada...", end="")

if not os.path.exists(DATA_SCORE_DIR):
    os.mkdir(DATA_SCORE_DIR)

df_score.to_csv( os.path.join(DATA_SCORE_DIR,"tb_score.csv"),
                 sep="|",
                 index=False)
print("ok.")