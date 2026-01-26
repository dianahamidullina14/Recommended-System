import pandas as pd
from sqlalchemy import create_engine

# строка подключения
db_url = "postgresql://robot-startml-ro:pheiph0hahj1Vaif@postgres.lab.karpov.courses:6432/startml"

# создаём подключение
engine = create_engine(db_url)

# смотрим все таблицы
# tables = pd.read_sql(
#     "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
#     engine
# )
# print(tables)

# table_post = pd.read_sql(
#     "SELECT *  FROM post",
#     engine
# )
# print(table_post.sample(10, random_state=42))

# table_user = pd.read_sql(
#     'SELECT *  FROM "user"',
#     engine
# )
# print(table_user.columns.tolist())

table_feed_act = pd.read_sql(
    'SELECT * FROM feed_action LIMIT 1',
    engine
)
print(table_feed_act.columns.tolist())
