import datetime

from pydantic import BaseModel, Field
from typing import List

'''
class UserGet(BaseModel):
    id: int
    gender: str
    age: int
    country: str
    city: str
    exp_group: int
    os: str
    source: str

    class Config:
        orm_mode = True


class FeedGet(BaseModel):
    id: int
    user_id: int
    post_id: int

    user: UserGet
    post: PostGet

    action: str
    time: datetime.datetime

    class Config:
        orm_mode=True
'''
class PostGet(BaseModel):
    id:int
    text: str
    topic: str

    class Config:
        orm_mode=True

class Response(BaseModel):
    recommendations: List[PostGet]
    exp_group: str
