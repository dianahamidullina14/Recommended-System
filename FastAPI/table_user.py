from database import Base, SessionLocal
from sqlalchemy import Column, String, Integer, func

class User(Base):
    __tablename__ = "user"
    __table_args__ = {"schema": "public"}

#['id', 'gender', 'age', 'country', 'city', 'exp_group', 'os', 'source']
    id = Column(Integer, primary_key = True)
    gender = Column(String)
    age = Column(Integer)
    country = Column(String)
    city = Column(String)
    exp_group = Column(Integer)
    os = Column(String)
    source = Column(String)

if __name__ == "__main__":
    session = SessionLocal()
    # запрос: фильтр exp_group = 3, группировка по (country, os)

    result = (
        session.query(User.country,
                      User.os,
                      func.count().label("cnt")
        )
        .filter(User.exp_group == 3)
        .group_by(User.country, User.os)
        .having(func.count()> 100)
        .order_by(func.count().desc())
        .all()
    )

    result_list = [(r.country, r.os, r.cnt) for r in result]
    print(result_list)