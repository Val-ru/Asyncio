import os

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, MappedColumn, mapped_column
from sqlalchemy import Integer, String, Float, JSON


from config import LIST_OF_KEYS

POSTGRES_USER = os.getenv("POSTGRES_USER", "swapi")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "secret")
POSTGRES_DB = os.getenv("POSTGRES_DB", "swapi")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5431")

PG_DSN = f"postgresql+asyncpg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}"
engine = create_async_engine(PG_DSN)
DbSession = async_sessionmaker(engine, expire_on_commit=False)

class Base(DeclarativeBase, AsyncAttrs):
    pass

class SwapiPeople(Base):
    __tablename__ = "swapi"

    id: MappedColumn[int] = mapped_column(Integer, primary_key=True)
    birth_year: MappedColumn[str] = mapped_column(String(30))
    eye_color: MappedColumn[str] = mapped_column(String(30))
    gender: MappedColumn[str] = mapped_column(String(30))
    hair_color: MappedColumn[str] = mapped_column(String(30))
    homeworld: MappedColumn[str] = mapped_column(String(255))
    mass: MappedColumn[str] = mapped_column(String(50))
    name: MappedColumn[str] = mapped_column(String(255))
    skin_color: MappedColumn[str] = mapped_column(String(30))
    films: MappedColumn[str] = mapped_column(String(1000), nullable=True)
    species: MappedColumn[str] = mapped_column(String(1000), nullable=True)
    vehicles: MappedColumn[str] = mapped_column(String(1000), nullable=True)
    starships:MappedColumn[str] = mapped_column(String(1000), nullable=True)

async def init_orm():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def close_orm():
    await engine.dispose()