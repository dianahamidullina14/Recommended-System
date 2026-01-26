#['user_id', 'post_id', 'action', 'time', 'id']
from database import Base, SessionLocal
from sqlalchemy import Column, String, Integer, func, ForeignKey,TIMESTAMP
from table_user import User
from table_post import Post
from sqlalchemy.orm import relationship

class Feed(Base):
    __tablename__ = "feed_action"
    __table_args__ = {"schema": "public"}

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("public.user.id"))
    post_id = Column(Integer, ForeignKey("public.post.id"))

    user = relationship(User)
    post = relationship(Post)

    action = Column(String)
    time = Column(TIMESTAMP)
