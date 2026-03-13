import os 
import sys
from typing import List, Tuple
from fastapi import FastAPI
from schema import PostGet, Response
from datetime import datetime
from sqlalchemy import create_engine
from loguru import logger
from catboost import CatBoostClassifier
import hashlib
import pandas as pd
import pickle



# Добавляем путь к установленным пакетам
#sys.path.insert(0, r'D:\PythonLibs')

print("Python paths:", sys.path)

app = FastAPI()

def batch_load_sql(query: str) -> pd.DataFrame:
    engine = create_engine(
        "postgresql://robot-startml-ro:pheiph0hahj1Vaif@"
        "postgres.lab.karpov.courses:6432/startml"
    )
   
    conn = engine.connect().execution_options(stream_results=True)
    chunks = []
    for chunk_dataframe in pd.read_sql(query, conn, chunksize=200000):
        chunks.append(chunk_dataframe)
    conn.close()
    return pd.concat(chunks, ignore_index=True)


def get_model_path(model_version: str) -> str:
    """
    Здесь мы модицифируем функцию так, чтобы иметь возможность загружать
    обе модели. При этом мы могли бы загружать и приципиально разные
    модели, так как никак не ограничены тем, какой код использовать.
    """
    print(os.environ)
    if os.environ.get("IS_LMS") == "1":  # проверяем где выполняется код в лмс, или локально. Немного магии
        model_path = f"/workdir/user_input/model_{model_version}"
    else:
        # Используем правильные имена файлов
        if model_version == "control":
            model_path = r"D:\Recommended-System\model\catboost_model"
        elif model_version == "test":
            model_path = r"D:\Recommended-System\model\catboost_model_test"
        else:
            model_path = fr"D:\Recommended-System\model\{model_version}"
    return model_path


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
        con="postgresql://robot-startml-ro:pheiph0hahj1Vaif@"
            "postgres.lab.karpov.courses:6432/startml"
    )

    logger.info("loading user features")
    user_features = pd.read_sql(
        """SELECT * FROM user_data""",
        con="postgresql://robot-startml-ro:pheiph0hahj1Vaif@"
            "postgres.lab.karpov.courses:6432/startml"
    )

    return [liked_posts, posts_features, user_features]

def load_models(model_version: str):
    model_path = get_model_path(model_version)
    try:
        loaded_model = CatBoostClassifier()
        loaded_model.load_model(model_path)
        logger.info(f"Model {model_version} loaded successfully in CBM format")
        return loaded_model
    except Exception as e:
        logger.error(f"Failed to load model {model_version}: {e}")
        raise


logger.info("loading model")
features = load_features()
logger.info("loading features")

# Проверяем существование файлов моделей перед загрузкой
model_control_path = get_model_path("control")
model_test_path = get_model_path("test")

if os.path.exists(model_control_path) and os.path.exists(model_test_path):
    model_control = load_models("control")
    model_test = load_models("test")
else:
    logger.warning("Model files not found. Using mock models.")
    # Здесь можно добавить заглушки для моделей
    model_control = None
    model_test = None

def debug_model_features():
    """Отладочная функция для просмотра признаков моделей"""
    logger.info("=== DEBUG MODEL FEATURES ===")
    
    # Для control модели
    if model_control is not None:
        try:
            # Пробуем разные способы получить признаки
            if hasattr(model_control, 'feature_names_'):
                control_features = model_control.feature_names_
                logger.info(f"Control model features: {control_features}")
                logger.info(f"Control model features count: {len(control_features)}")
            else:
                logger.info("Control model: feature_names_ not available")
                # Попробуем получить через get_params
                logger.info(f"Control model params: {model_control.get_params()}")
        except Exception as e:
            logger.info(f"Control model: error getting features: {e}")
    
    # Для test модели
    if model_test is not None:
        try:
            if hasattr(model_test, 'feature_names_'):
                test_features = model_test.feature_names_
                logger.info(f"Test model features: {test_features}")
                logger.info(f"Test model features count: {len(test_features)}")
            else:
                logger.info("Test model: feature_names_ not available")
                logger.info(f"Test model params: {model_test.get_params()}")
        except Exception as e:
            logger.info(f"Test model: error getting features: {e}")
    
    logger.info("=== END DEBUG ===")

# Вызываем функцию после загрузки моделей
debug_model_features()

logger.info("service is up and running")

SALT = "my_salt"

def get_user_group(id: int) -> str:
    value_str = str(id) + SALT
    value_num = int(hashlib.md5(value_str.encode()).hexdigest(), 16)
    percent = value_num % 100
    if percent < 50:
        return "control"
    elif percent < 100:
        return "test"
    return "unknown"

def calculate_features(id: int, time: datetime, group: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    logger.info(f"user_id: {id}, group: {group}")
    logger.info("reading features")
    
    # Получаем признаки пользователя
    user_features = features[2].loc[features[2].user_id == id]
    
    if user_features.empty:
        logger.warning(f"No features found for user {id}")
        return pd.DataFrame(), pd.DataFrame()
    
    user_features = user_features.drop("user_id", axis=1)

    logger.info("dropping columns")
    
    # Берем признаки постов
    posts_features = features[1].copy()
    logger.info(f"Posts features columns: {list(posts_features.columns)}")

    logger.info("zipping everything")
    add_user_features = dict(zip(user_features.columns, user_features.values[0]))
    logger.info(f"User features: {add_user_features}")
    
    logger.info("assigning everything")
    user_post_features = posts_features.assign(**add_user_features)
    
    # Сохраняем post_id для индекса
    user_post_features = user_post_features.set_index("post_id")

    logger.info("add date info")
    user_post_features["hour"] = time.hour
    user_post_features["month"] = time.month
    
    # Категориальные признаки
    categorical_features = ['topic', 'country', 'city', 'os', 'source']
    
    # Преобразование категориальных признаков в строки
    logger.info("Преобразование категориальных признаков в строки")
    for col in categorical_features:
        if col in user_post_features.columns:
            if user_post_features[col].isna().any():
                user_post_features[col] = user_post_features[col].fillna('unknown')
            user_post_features[col] = user_post_features[col].astype(str)
            logger.info(f"  {col}: {user_post_features[col].dtype}, unique values: {user_post_features[col].nunique()}")
    
    # Заполняем пропуски в числовых колонках
    numeric_cols = user_post_features.select_dtypes(include=['float64', 'int64']).columns
    if len(numeric_cols) > 0:
        user_post_features[numeric_cols] = user_post_features[numeric_cols].fillna(0)
    
    # Для тестовой модели - создаем MeanTfIdf
    if group == "test" and 'MeanTfIdf' not in user_post_features.columns:
        if 'TotalTfIdf' in user_post_features.columns:
            # Примерное значение MeanTfIdf
            user_post_features['MeanTfIdf'] = user_post_features['TotalTfIdf'] / 10
            logger.info("Created MeanTfIdf from TotalTfIdf")
    
    # Для контрольной модели - переименовываем DistanceTo... в правильный формат
    if group == "control":
        # Переименовываем DistanceTo1thCluster -> DistanceToCluster_1 и т.д.
        for i in range(1, 16):
            old_name = f'DistanceTo{i}thCluster'
            new_name = f'DistanceToCluster_{i-1}'  # В control модели нумерация с 0
            if old_name in user_post_features.columns and new_name not in user_post_features.columns:
                user_post_features[new_name] = user_post_features[old_name]
                logger.info(f"Renamed {old_name} to {new_name}")
    
    # Выбираем только нужные признаки в правильном порядке
    if group == "control":
        expected_features = [
            'topic', 'TextCluster', 'DistanceToCluster_0', 'DistanceToCluster_1', 
            'DistanceToCluster_2', 'DistanceToCluster_3', 'DistanceToCluster_4', 
            'DistanceToCluster_5', 'DistanceToCluster_6', 'DistanceToCluster_7', 
            'DistanceToCluster_8', 'DistanceToCluster_9', 'DistanceToCluster_10', 
            'DistanceToCluster_11', 'DistanceToCluster_12', 'DistanceToCluster_13', 
            'DistanceToCluster_14', 'TotalTfIdf', 'MaxTfIdf', 'MinTfIdf', 'gender', 
            'age', 'country', 'city', 'exp_group', 'os', 'source', 'hour', 'month'
        ]
    else:  # test
        expected_features = [
            'topic', 'TotalTfIdf', 'MaxTfIdf', 'MeanTfIdf', 'TextCluster', 
            'DistanceTo1thCluster', 'DistanceTo2thCluster', 'DistanceTo3thCluster', 
            'DistanceTo4thCluster', 'DistanceTo5thCluster', 'DistanceTo6thCluster', 
            'DistanceTo7thCluster', 'DistanceTo8thCluster', 'DistanceTo9thCluster', 
            'DistanceTo10thCluster', 'DistanceTo11thCluster', 'DistanceTo12thCluster', 
            'DistanceTo13thCluster', 'DistanceTo14thCluster', 'DistanceTo15thCluster', 
            'gender', 'age', 'country', 'city', 'exp_group', 'os', 'source', 'hour', 'month'
        ]
    
    # Проверяем наличие всех признаков
    available_features = [f for f in expected_features if f in user_post_features.columns]
    missing_features = [f for f in expected_features if f not in user_post_features.columns]
    
    if missing_features:
        logger.warning(f"Missing features for {group}: {missing_features}")
        # Создаем недостающие признаки с нулевыми значениями
        for f in missing_features:
            user_post_features[f] = 0
    
    # Выбираем признаки в правильном порядке
    user_post_features = user_post_features[expected_features]
    
    logger.info(f"Final features for {group}: {list(user_post_features.columns)}")
    logger.info(f"Number of features: {len(user_post_features.columns)}")
    logger.info(f"Number of posts: {len(user_post_features)}")

    return user_features, user_post_features

def get_recommended_feed(id: int, time: datetime, limit: int) -> Response:
    user_group = get_user_group(id=id)
    logger.info(f"user group {user_group}")
    
    # Выбираем модель по группе
    if user_group == "control":
        model = model_control
    elif user_group == "test":
        model = model_test
    else:
        raise ValueError("unknown group")
    
    if model is None:
        raise ValueError(f"Model for group {user_group} is not loaded")
    
    # Получаем признаки
    user_features, user_post_features = calculate_features(id=id, time=time, group=user_group)
    
    if user_post_features.empty:
        logger.warning(f"No features for user {id}, returning empty recommendations")
        return Response(recommendations=[], exp_group=user_group)

    logger.info(f"predicting with {user_group} model")
    logger.info(f"Data shape: {user_post_features.shape}")
    
    # Для тестовой модели убедимся что есть MeanTfIdf
    if user_group == "test" and 'MeanTfIdf' not in user_post_features.columns:
        logger.error("MeanTfIdf not found for test model!")
        # Создаем заглушку
        if 'TotalTfIdf' in user_post_features.columns:
            user_post_features['MeanTfIdf'] = user_post_features['TotalTfIdf'] / user_post_features.shape[1]
        else:
            user_post_features['MeanTfIdf'] = 0
    
    try:
        # Пробуем предсказать без указания категориальных признаков
        # (они уже преобразованы в строки)
        predicts = model.predict_proba(user_post_features)[:, 1]
        logger.info(f"Predictions shape: {predicts.shape}")
        logger.info(f"Predictions range: [{predicts.min():.4f}, {predicts.max():.4f}]")
        
    except Exception as e:
        logger.error(f"Prediction error: {e}")
        
        # Пробуем другой подход - используем только числовые признаки
        logger.info("Trying with only numeric features...")
        numeric_cols = user_post_features.select_dtypes(include=['float64', 'int64']).columns
        logger.info(f"Numeric columns: {list(numeric_cols)}")
        
        try:
            predicts = model.predict_proba(user_post_features[numeric_cols])[:, 1]
        except Exception as e2:
            logger.error(f"Second attempt failed: {e2}")
            raise
    
    # Добавляем предсказания в DataFrame
    user_post_features["predicts"] = predicts

    # Исключаем уже лайкнутые посты
    logger.info("filtering liked posts")
    liked_posts = features[0]
    liked_posts = liked_posts[liked_posts.user_id == id].post_id.values
    logger.info(f"User liked {len(liked_posts)} posts")
    
    filtered_posts = user_post_features[~user_post_features.index.isin(liked_posts)]

    if filtered_posts.empty:
        logger.warning(f"No unliked posts for user {id}")
        return Response(recommendations=[], exp_group=user_group)

    # Выбираем топ-limit постов с наибольшей вероятностью
    recommended_posts = filtered_posts.sort_values('predicts', ascending=False).head(limit)
    logger.info(f"Selected {len(recommended_posts)} recommendations")

    # Формируем результат
    post_info = features[1].set_index('post_id')
    recommendations = []
    
    for post_id, row in recommended_posts.iterrows():
        if post_id in post_info.index:
            text = post_info.loc[post_id, 'text'] if 'text' in post_info.columns else "No text"
            topic = post_info.loc[post_id, 'topic'] if 'topic' in post_info.columns else "unknown"
            
            # Обрезаем слишком длинный текст
            if len(text) > 200:
                text = text[:200] + "..."
                
            recommendations.append(PostGet(
                id=post_id, 
                text=text, 
                topic=topic
            ))
            
            logger.debug(f"  Post {post_id}: topic={topic}, prob={row['predicts']:.4f}")

    return Response(
        recommendations=recommendations,
        exp_group=user_group,
    )

@app.get("/post/recommendations/", response_model=Response)
def recommended_posts(id: int, time: datetime, limit: int = 10) -> Response:
    return get_recommended_feed(id, time, limit)