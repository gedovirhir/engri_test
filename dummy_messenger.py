from pydantic import BaseModel, ConfigDict
from datetime import datetime
import uvicorn
import asyncio
import random

import fastapi
from fastapi import Depends

from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker
from sqlalchemy import String, func, Sequence, select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, AsyncAttrs, async_sessionmaker

from typing import List, AsyncGenerator, Annotated

from core import REPLICA_COUNT, SERVER_PORT

class Base(AsyncAttrs, DeclarativeBase):
    pass

class UserMessage(Base):
    __tablename__ = 'user_message'
    
    id: Mapped[int] = mapped_column(primary_key=True)
    username: Mapped[str] = mapped_column(String(30), index=True)
    text: Mapped[str] = mapped_column(String(255))
    created_at: Mapped[datetime] = mapped_column(server_default=func.current_timestamp(), index=True)
    serial_number: Mapped[int] = mapped_column(index=True)

def get_user_seq(username: str) -> Sequence:
    return Sequence(f"{username}_serial_num", start=1, increment=1)

a_engine = create_async_engine("sqlite+aiosqlite:///database.db", echo=True, connect_args={'timeout': 15})

SessionLocal = async_sessionmaker(
    a_engine, 
    expire_on_commit=False,
    autoflush=False,
)

async def get_session() -> AsyncGenerator[AsyncSession, None]:
    async with SessionLocal() as session:
        yield session

DbSession = Annotated[AsyncSession, Depends(get_session)]

async def db_create_all():
    async with a_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

class ProjectBase(BaseModel):
    model_config = ConfigDict(
        from_attributes=True
    )

class Message(BaseModel):
    username: str
    text: str
    
class MessageResponse(Message):
    created_at: datetime
    serial_number: int

app = fastapi.FastAPI()

@app.post('/message', response_model=List[MessageResponse])
async def post_message(message: Message, db: DbSession) -> list:
    async with db.begin():
        #next_n = await db.execute(get_user_seq())
        
        next_n = (
            select(func.coalesce(func.max(UserMessage.serial_number), 0))
            .where(UserMessage.username == message.username)
            #.with_for_update()
            .scalar_subquery()
        )
        
        new_message = UserMessage(username=message.username, text=message.text, serial_number=next_n + 1)
        db.add(new_message)
        
        await db.commit()
    
    await db.refresh(new_message)
    
    old_messages = await db.scalars(
        select(UserMessage)
        .where(UserMessage.username == message.username, UserMessage.serial_number <= new_message.serial_number)\
        .order_by(UserMessage.serial_number.desc())\
        .limit(10)
    )
    
    old_messages_list = old_messages.all()
    
    return old_messages_list

def run_server(port: int = 8000, replicas: int = 10):
    uvicorn.run(
        "dummy_messenger:app", 
        host="0.0.0.0", 
        reload=False, 
        port=port,
        workers=replicas,
    )
    
if __name__ == "__main__":
    asyncio.run(db_create_all())
    run_server(port=SERVER_PORT, replicas=REPLICA_COUNT)
