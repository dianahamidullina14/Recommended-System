from database import Base, SessionLocal
from sqlalchemy import Column, String, Integer

class Post(Base):
    __tablename__ = "post"
    __table_args__ = {"schema": "public"}

    id = Column(Integer, primary_key=True)
    text = Column(String)
    topic = Column(String)

if __name__ == "__main__":
    session = SessionLocal()
    id_list =[]
    result = (
        session.query(Post.id)
        .filter(Post.topic == "business")
        .order_by(Post.id.desc())
        .limit(10)
        .all()
    )
    for i in result:
        id_list.append(i[0])

    print(id_list)
#более простой вариант  print([p[0] for p in posts])
