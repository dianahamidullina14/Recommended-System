import os 
import sys
# Добавляем путь к установленным пакетам
sys.path.insert(0, r'D:\PythonLibs')

# Убедитесь, что Python может найти пакеты
print("Python paths:", sys.path)
import pandas as pd
from typing import List
from fastapi import FastAPI
from schema import PostGet
from datetime import datetime
from sqlalchemy import create_engine
from loguru import logger
#from catboost import CatBoostClassifier



app = FastAPI()



def batch_load_sql(query: str):
    engine = create_engine(
    "postgresql://robot-startml-ro:pheiph0hahj1Vaif@"
    "postgres.lab.karpov.courses:6432/startml"
    )
   
    conn = engine.connect().execution_options(
        stream_results=True)
    chunks = []
    for chunk_dataframe in pd.read_sql(query, conn, chunksize=200000):
        chunks.append(chunk_dataframe)
    conn.close()
    return pd.concat(chunks, ignore_index=True)


def get_model_path(path: str) -> str:
    if os.environ.get("IS_LMS") == "1":
        MODEL_PATH =  '/workdir/user_input/model'
    else:
        MODEL_PATH= path
    return MODEL_PATH


def load_features():
    logger.info("loading liked posts")
    liked_posts_query = """
        SELECT distinct post_id, user_id
        FROM feed_data
        WHERE action='like'
        """
    liked_posts = batch_load_sql(liked_posts_query)

    logger.info("loading posts features")
    posts_features = pd.read_sql(
        """SELECT * FROM post_info_features""", 
        con = 
        "postgresql://robot-startml-ro:pheiph0hahj1Vaif@"
        "postgres.lab.karpov.courses:6432/startml"
    )

    logger.info("loading user features")
    user_features = pd.read_sql(
        """SELECT * FROM user_data""",
        con = 
        "postgresql://robot-startml-ro:pheiph0hahj1Vaif@"
        "postgres.lab.karpov.courses:6432/startml"
    )

    return [liked_posts, posts_features, user_features]


def load_models():
    print("INFO: Используется временная заглушка CatBoost")
    import numpy as np
    
    class MockCatBoost:
        def predict_proba(self, X):
            # Реалистичная заглушка для тестирования API
            n = X.shape[0]
            probs = np.zeros((n, 2))
            
            # Первые 30% - хорошие посты
            n_top = int(n * 0.3)
            probs[:n_top, 1] = np.linspace(0.9, 0.7, n_top)
            probs[n_top:, 1] = np.linspace(0.4, 0.1, n - n_top)
            probs[:, 0] = 1 - probs[:, 1]
            
            return probs
    
    return MockCatBoost()


logger.info("loading model")
model = load_models()
logger.info("loading features")
features = load_features()
logger.info("service is up and running")



def get_recommended_feed(id: int, time: datetime, limit: int):

    logger.info(f"user_id {id}")
    logger.info("reading features")
    user_features = features[2].loc[features[2].user_id == id]
    user_features = user_features.drop('user_id', axis=1)

    logger.info("dropping columns")
    posts_features = features[1].drop(['index', 'text'], axis=1)
    content = features[1][['post_id', 'text', 'topic']]

    logger.info("zipping everything")
    add_user_features = dict(zip(user_features.columns, user_features.values[0]))
    logger.info("assigning evetything")
    user_post_features = posts_features.assign(**add_user_features)
    user_post_features = user_post_features.set_index('post_id')

    logger.info("add time info")
    user_post_features['hour'] = time.hour
    user_post_features['month'] = time.month

    logger.info("predicting")
    predicts = model.predict_proba(user_post_features)[:, 1]
    user_post_features["predicts"] = predicts


    logger.info("deleting liked posts")
    liked_posts = features[0]
    liked_posts = liked_posts[liked_posts.user_id == id].post_id.values
    filtered_ = user_post_features[~user_post_features.index.isin(liked_posts)]

    recommended_posts = filtered_.sort_values('predicts')[-limit:].index

    return [
        PostGet(**{
            "id": i,
            "text": content[content.post_id == i].text.values[0],
            "topic": content[content.post_id == i].topic.values[0]
        }) for i in recommended_posts
    ]

@app.get("/post/recommendations/", response_model=List[PostGet])
def recommended_posts(id: int, time: datetime, limit: int = 10) -> List[PostGet]:
    return get_recommended_feed(id, time, limit)
