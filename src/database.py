from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.config import settings
from src.models import Base

engine = create_engine(
    settings.database_url,
    pool_pre_ping=True,  # Verifica conexión viva antes de transacciones
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

