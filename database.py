from dotenv import load_dotenv
from sqlmodel import create_engine, SQLModel, Session
import os

load_dotenv()
# DATABASE_URL = os.environ.get("")
DATABASE_URL = os.getenv('POSTGRES_DB')
engine = create_engine(DATABASE_URL, echo=True)


def init_db():
    SQLModel.metadata.create_all(engine)


def get_session():
    with Session(engine) as session:
        yield session